import logging
from typing import List

import pandas as pd

from data.dataset import Dataset, Metric


class JobReportMatcher:
    """Matches JobMonitoring to WMArchive job reports."""

    def __init__(self, timestamp_tolerance=10, time_grouping_freq='D'):
        """Create a new matching object with supplied preferences.

        :param timestamp_tolerance: The tolerance to be used when comparing time stamps for potential matches.
        :param time_grouping_freq: The frequency with which to group entries by prior to trying to match them.
        """
        self.timestamp_tolerance = timestamp_tolerance
        self.time_grouping_freq = time_grouping_freq

    def match_reports(self, jmset, wmset, use_files=True):
        """Match reports from jobmonitoring and WMArchive data.

        :param jmset: The JobMonitoring dataset.
        :param wmset: The WMArchive dataset.
        :param use_files: If true, use comparisons between file lists to identify potential matches.
        :return: A data frame containing matches from the index columns of the two datasets.
        """
        unmatched_jmdf = jmset.df.copy()
        unmatched_wmdf = wmset.df.copy()

        # TODO Make this generic to exclude data from either data set
        # Exclude crab jobs as they are not present in the other data set
        unmatched_jmdf = unmatched_jmdf[unmatched_jmdf[jmset.col(Metric.SUBMISSION_TOOL)] != 'crab3']

        logging.debug("Removing all workflows not present in both data sets.")

        # Remove all workflows that are not present in the other data set
        jm_workflows = set(unmatched_jmdf[jmset.col(Metric.WORKFLOW)].unique())
        wm_workflows = set(unmatched_wmdf[wmset.col(Metric.WORKFLOW)].unique())
        in_both = jm_workflows & wm_workflows

        unmatched_jmdf = unmatched_jmdf[unmatched_jmdf[jmset.col(Metric.WORKFLOW)].isin(in_both)]
        unmatched_wmdf = unmatched_wmdf[unmatched_wmdf[wmset.col(Metric.WORKFLOW)].isin(in_both)]

        only_wm = wm_workflows - jm_workflows
        only_jm = jm_workflows - wm_workflows

        logging.debug("Removed {} workflows not present WMArchive data.".format(len(only_wm)))
        logging.debug("Removed {} workflows not present in Jobmonitoring data.".format(len(only_jm)))

        # Group by set frequency to simplify matching.
        jm_grouped = self.group_by_time(unmatched_jmdf, [jmset.col(Metric.STOP_TIME)], freq=self.time_grouping_freq)
        wm_grouped = self.group_by_time(unmatched_wmdf, [wmset.col(Metric.STOP_TIME)], freq=self.time_grouping_freq)

        match_list = []
        total_compared = 0
        total = unmatched_jmdf.shape[0]

        for key, jm_group in jm_grouped:
            try:
                wm_group = wm_grouped.get_group(key)
            except KeyError:
                # Group is not present in other frame
                continue

            if jm_group.empty or wm_group.empty:
                continue

            group_matches = self.match_on_cpu_time(jmset, wmset, jm_group, wm_group)
            match_list.append(group_matches)

            total_compared += len(jm_group)
            logging.debug(
                "Found {} matches (of {} WM, {} JM, ".format(len(group_matches), len(wm_group), len(jm_group)) +
                "{:.4}% compared).".format(100 * total_compared / total)
            )

        matches = pd.concat(match_list)

        logging.debug(
            "{} duplicates in Jobmonitoring matches.".format(matches[unmatched_jmdf.index.name].duplicated().sum()))
        logging.debug(
            "{} duplicates in WMArchive matches.".format(matches[unmatched_wmdf.index.name].duplicated().sum()))

        # Drop all matches from unmatched jobs
        unmatched_jmdf = unmatched_jmdf.drop(matches[unmatched_jmdf.index.name])
        unmatched_wmdf = unmatched_wmdf.drop(matches[unmatched_wmdf.index.name])

        logging.info(
            "Found {} matches, {} WMArchive jobs unmatched,".format(matches.shape[0], unmatched_wmdf.shape[0]) +
            "{} Jobmonitoring jobs unmatched.".format(unmatched_jmdf.shape[0]))

        if use_files and 'files' in jmset.extra_dfs and 'files' in jmset.extra_dfs:
            logging.info("Matching on files.")

            file_matches = self.match_on_files(jmset, wmset, unmatched_jmdf, unmatched_wmdf)
            matches = matches.append(file_matches)

            unmatched_jmdf = unmatched_jmdf.drop(matches[unmatched_jmdf.index.name], errors='ignore')
            unmatched_wmdf = unmatched_wmdf.drop(matches[unmatched_wmdf.index.name], errors='ignore')

            logging.debug(
                "Found {} matches, {} unmatched in Jobmonitoring,".format(matches.shape[0], unmatched_jmdf.shape[0]) +
                "{} unmatched in WMArchive jobs.".format(unmatched_wmdf.shape[0]))

        # Directly match on workflow with remaining data
        workflow_matches = self.match_on_workflow(unmatched_jmdf, unmatched_wmdf, jmset, wmset)
        matches.append(workflow_matches)

        return matches.reset_index(drop=True)

    def filter_matches(self, matches, jm_dataset: Dataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_'):

        timestamp_metrics = [Metric.START_TIME, Metric.STOP_TIME]

        for metric in timestamp_metrics:
            jmdf_ts_col = jmdf_prefix + jm_dataset.col(metric)
            wmdf_ts_col = wmdf_prefix + wm_dataset.col(metric)
            matches = matches[
                (self.timestamp_diff_series(matches[jmdf_ts_col], matches[wmdf_ts_col]) < self.timestamp_tolerance)  # |
                # (matches[jmdf_ts_col].isnull()) | (matches[wmdf_ts_col].isnull())
            ]

        jm_workflow_col = jmdf_prefix + jm_dataset.col(Metric.WORKFLOW)
        wm_workflow_col = wmdf_prefix + wm_dataset.col(Metric.WORKFLOW)

        # Only accept jobs that match in their workflow
        matches = matches[matches[jm_workflow_col] == matches[wm_workflow_col]]

        return matches

    def filter_by_timestamp(self, left, matches, left_dataset, right_dataset, left_prefix='', right_prefix=''):

        timestamp_metrics = [Metric.START_TIME, Metric.STOP_TIME]

        for metric in timestamp_metrics:
            left_timestamp = left[left_prefix + left_dataset.col(metric)]
            right_colname = right_prefix + right_dataset.col(metric)

            if pd.isna(left_timestamp):
                # Filter timestamps on the right with specified tolerance, or keep entries if the left timestamp is null
                matches = matches[matches.apply(
                    lambda x: self.timestamp_diff(left_timestamp, x[right_colname]) < self.timestamp_tolerance,
                    axis=1) | matches[right_colname].isnull()]

        return matches

    def match_on_cpu_time(self, jm_dataset: Dataset, wm_dataset: Dataset, jm_subset=None, wm_subset=None):
        jmdf = jm_subset if jm_subset is not None else jm_dataset.df
        wmdf = wm_subset if wm_subset is not None else wm_dataset.df

        # Round CPU time to account for rounding errors while matching float values
        jmdf['cpuApprox'] = jmdf[jm_dataset.col(Metric.CPU_TIME)].round()
        wmdf['cpuApprox'] = wmdf[wm_dataset.col(Metric.CPU_TIME)].round()

        jmdf_index = jmdf.index.name
        wmdf_index = wmdf.index.name

        self.prefix_columns(jmdf, 'jmdf_')
        self.prefix_columns(wmdf, 'wmdf_')

        matches = jmdf.reset_index().merge(wmdf.reset_index(), left_on='jmdf_cpuApprox', right_on='wmdf_cpuApprox')

        filtered = self.filter_matches(matches, jm_dataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_')

        perfect_matches = filtered.groupby(jmdf_index).filter(lambda x: len(x) == 1)

        return perfect_matches[[jmdf_index, wmdf_index]]

    def match_on_workflow(self, jmdf, wmdf, jmset: Dataset, wmset: Dataset):
        jm_grouped = jmdf.groupby(jmset.col(Metric.WORKFLOW))
        wm_grouped = wmdf.groupby(wmset.col(Metric.WORKFLOW))

        total_compared = 0
        total = jmdf.shape[0]

        matches = {jmdf.index.name: [], wmdf.index.name: []}

        for key, jm_group in jm_grouped:
            try:
                wm_group = wm_grouped.get_group(key)
            except KeyError:
                # Group is not present in other frame
                continue

            if jm_group.empty or wm_group.empty:
                continue

            self.prefix_columns(jm_group, 'jmdf_')
            self.prefix_columns(wm_group, 'wmdf_')

            group_match_count = 0
            for jm_index, jm_job in jm_group.iterrows():
                matching_entries = self.filter_by_timestamp(jm_job, wm_group, jmset, wmset, left_prefix='jmdf_',
                                                            right_prefix='wmdf_')

                if len(matching_entries) == 1:
                    # Perfect match found, insert into match list
                    matches.get(jmdf.index.name).append(jm_job.index)
                    matches.get(wmdf.index.name).append(matching_entries.iloc[0].index)

                    group_match_count += 1

            total_compared += len(jm_group)

        match_df = pd.DataFrame.from_dict(matches)
        return match_df

    def match_on_files(self, jm_dataset: Dataset, wm_dataset: Dataset, jm_subset=None, wm_subset=None):
        jmdf = self.pick_subset(jm_dataset, jm_subset)
        wmdf = self.pick_subset(wm_dataset, wm_subset)

        jm_index = jmdf.index.name
        wm_index = wmdf.index.name

        jm_files = jm_dataset.extra_dfs.get('files')
        wm_files = wm_dataset.extra_dfs.get('files')

        if jm_files is None or wm_files is None:
            # Missing file information, skipping matching
            return None

        jm_files = jm_files[jm_files[jm_index].isin(jmdf.index)]
        wm_files = wm_files[wm_files[wm_index].isin(wmdf.index)]

        possible_matches = self._all_file_matches(jm_files, wm_files, jm_index=jm_index, wm_index=wm_index)

        if possible_matches.empty:
            return None

        self.prefix_columns(jmdf, 'jmdf_')
        self.prefix_columns(wmdf, 'wmdf_')

        matched_jobs = possible_matches.join(jmdf, on=jm_index).join(wmdf, on=wm_index)
        filtered = self.filter_matches(matched_jobs, jm_dataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_')

        perfect_matches = filtered.groupby(jm_index).filter(lambda x: len(x) == 1)
        return perfect_matches[[jm_index, wm_index]]

    @staticmethod
    def pick_subset(dataset: Dataset, subset=None):
        return subset if subset is not None else dataset.df

    @staticmethod
    def prefix_columns(df, prefix=''):
        df.columns = map(lambda x: prefix + x, df.columns)

    @staticmethod
    def timestamp_diff(ts1, ts2):
        return abs((ts1 - ts2).total_seconds())

    @staticmethod
    def timestamp_diff_series(ts_series1, ts_series2):
        return (ts_series1 - ts_series2).dt.total_seconds().abs()

    @staticmethod
    def group_by_time(df, ts_cols: List[str], freq):
        df = df.copy()
        for timestamp_col in ts_cols:
            df[timestamp_col] = df[timestamp_col].dt.floor(freq)

        return df.groupby(ts_cols)

    @staticmethod
    def _all_file_matches(jm_files, wm_files, file_col='FileName', jm_index='UniqueID', wm_index='wmaid'):
        matches = jm_files.merge(wm_files, on=file_col, how='inner')

        if matches.empty:
            return pd.DataFrame()
        else:
            return matches[[jm_index, wm_index]].drop_duplicates()