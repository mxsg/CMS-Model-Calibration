from typing import List

import pandas as pd

from ..data.dataset import JobsDataset, Metrics


class JobReportMatcher:

    def __init__(self, timestamp_tolerance=10):
        self.timestamp_tolerance = timestamp_tolerance

    def match_reports(self, jmset, wmset, jmfiles=None, wmfiles=None, cached_matches=None):
        unmatched_jmdf = jmset.jobs.copy()
        unmatched_wmdf = wmset.jobs.copy()

        print(jmset.metrics)
        print(wmset.metrics)

        # TODO Make this generic!
        # Exclude crab jobs as they are not present in the other data set
        unmatched_jmdf = unmatched_jmdf[unmatched_jmdf[jmset.col(Metrics.SUBMISSION_TOOL)] != 'crab3']

        # TODO Check whether this is valid, are there any matches with differing workflows?

        print("Removing all workflow not present in both data sets.")

        # Remove all workflows that are not present in the other data set
        jm_workflows = set(unmatched_jmdf[jmset.col(Metrics.WORKFLOW)])
        wm_workflows = set(unmatched_wmdf[wmset.col(Metrics.WORKFLOW)])
        in_both = jm_workflows & wm_workflows

        unmatched_jmdf = unmatched_jmdf[unmatched_jmdf[jmset.col(Metrics.WORKFLOW)].isin(in_both)]
        unmatched_wmdf = unmatched_wmdf[unmatched_wmdf[wmset.col(Metrics.WORKFLOW)].isin(in_both)]

        print("Removed all workflow not present in both data sets.")

        jm_grouped = self.group_by_time(unmatched_jmdf, [jmset.col(Metrics.STOP_TIME)], freq='D')
        wm_grouped = self.group_by_time(unmatched_wmdf, [wmset.col(Metrics.STOP_TIME)], freq='D')

        match_list = []
        total_compared = 0
        total = unmatched_jmdf.shape[0]

        for key, jm_group in jm_grouped:
            # try:
            wm_group = wm_grouped.get_group(key)
            # except:
            #     # Group is not present in other frame
            #     continue

            if jm_group.empty or wm_group.empty:
                continue

            print("wmdf {}, jmdf {}".format(len(jm_group), len(wm_group)))

            group_matches = self.match_on_cpu_time(jmset, wmset, jm_group, wm_group)
            match_list.append(group_matches)
            print("Found {} matches".format(len(group_matches)))
            total_compared += len(jm_group)
            print("total compared {}, fraction {}".format(total_compared, total_compared / total))

        matches = pd.concat(match_list)
        print("Duplicates in jmdf matches: {}".format(matches[unmatched_jmdf.index.name].duplicated().sum()))
        print("Duplicates in wmdf matches: {}".format(matches[unmatched_wmdf.index.name].duplicated().sum()))

        unmatched_jmdf = unmatched_jmdf.drop(matches[unmatched_jmdf.index.name])
        unmatched_wmdf = unmatched_wmdf.drop(matches[unmatched_wmdf.index.name])

        print("Matches {}, unmatched from jmdf {}, from wmdf {}".format(matches.shape[0], unmatched_jmdf.shape[0],
                                                                        unmatched_wmdf.shape[0]))

        self.match_on_workflow(unmatched_jmdf, unmatched_wmdf, jmset.metrics, wmset.metrics)

        if jmfiles is not None and wmfiles is not None:
            print("Matching on files.")

            file_matches = self.match_on_files(jmset, wmset, jmfiles, wmfiles, unmatched_jmdf, unmatched_wmdf)
            matches = matches.append(file_matches)

            unmatched_jmdf = unmatched_jmdf.drop(matches[unmatched_jmdf.index.name])
            unmatched_wmdf = unmatched_wmdf.drop(matches[unmatched_wmdf.index.name])

            print("Matches {}, unmatched from jmdf {}, from wmdf {}".format(matches.shape[0], unmatched_jmdf.shape[0],
                                                                            unmatched_wmdf.shape[0]))

        return pd.concat(match_list)

    def filter_matches(self, matches, jm_dataset: JobsDataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_'):

        timestamp_metrics = [Metrics.START_TIME, Metrics.STOP_TIME]

        for metric in timestamp_metrics:
            jmdf_ts_col = jmdf_prefix + jm_dataset.col(metric)
            wmdf_ts_col = wmdf_prefix + wm_dataset.col(metric)
            matches = matches[
                (self.abs_timestamp_diff(matches[jmdf_ts_col], matches[wmdf_ts_col]) < self.timestamp_tolerance) |
                (matches[jmdf_ts_col].isnull()) | (matches[wmdf_ts_col].isnull())]

        jm_workflow_col = jmdf_prefix + jm_dataset.col(Metrics.WORKFLOW)
        wm_workflow_col = wmdf_prefix + wm_dataset.col(Metrics.WORKFLOW)

        # Only accept jobs that match in their workflow
        matches = matches[matches[jm_workflow_col] == matches[wm_workflow_col]]

        return matches

    def filter_by_timestamp(self, left, matches, left_metrics, right_metrics):

        timestamp_metrics = [Metrics.START_TIME, Metrics.STOP_TIME]

        for metric in timestamp_metrics:
            left_timestamp = left[left_metrics.get(metric)]

            matches = matches[
                (self.abs_timestamp_diff(left_timestamp, matches[right_metrics.col(metric)]) < self.timestamp_tolerance) |
                (matches[left_timestamp].isnull()) | (matches[right_metrics.col(metric)].isnull())]

        return matches


    def match_on_cpu_time(self, jm_dataset: JobsDataset, wm_dataset: JobsDataset, jm_subset=None, wm_subset=None):

        jmdf = jm_subset if jm_subset is not None else jm_dataset.jobs
        wmdf = wm_subset if wm_subset is not None else wm_subset.jobs

        jmdf['cpuApprox'] = jmdf[jm_dataset.col(Metrics.CPU_TIME)].round()
        wmdf['cpuApprox'] = wmdf[wm_dataset.col(Metrics.CPU_TIME)].round()

        jmdf_index = jmdf.index.name
        wmdf_index = wmdf.index.name

        self.prefix_columns(jmdf, 'jmdf_')
        self.prefix_columns(wmdf, 'wmdf_')

        matches = jmdf.reset_index().merge(wmdf.reset_index(), left_on='jmdf_cpuApprox', right_on='wmdf_cpuApprox')

        filtered = self.filter_matches(matches, jm_dataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_')

        perfect_matches = filtered.groupby(jmdf_index).filter(lambda x: len(x) == 1)

        return perfect_matches[[jmdf_index, wmdf_index]]

    def match_on_workflow(self, jmdf, wmdf, jm_metrics: Metrics, wm_metrics: Metrics):

        jm_grouped = jmdf.groupby(jm_metrics.get(Metrics.WORKFLOW))
        wm_grouped = wmdf.groupby(wm_metrics.get(Metrics.WORKFLOW))

        total_compared = 0
        total = jmdf.shape[0]

        match_list = []

        for key, jm_group in jm_grouped:
            # try:
            wm_group = wm_grouped.get_group(key)
            # except:
            #     # Group is not present in other frame
            #     continue

            if jm_group.empty or wm_group.empty:
                continue

            print("wmdf {}, jmdf {}".format(len(jm_group), len(wm_group)))

            self.prefix_columns(jm_group, 'jmdf_')
            self.prefix_columns(wm_group, 'wmdf_')

            group_matches = []
            for jm_index, jm_job in jm_group.iterrows():
                matching_entries = self.filter_by_timestamp(jm_job, wm_group, jm_metrics, wm_metrics)

                if len(matching_entries) == 1:
                    group_matches.append((jm_job.index, matching_entries.iloc[0].index))

            match_list += group_matches
            print("Found {} matches".format(len(group_matches)))
            total_compared += len(jm_group)
            print("total compared {}, fraction {}".format(total_compared, total_compared / total))

        return match_list


    def match_on_files(self, jm_dataset: JobsDataset, wm_dataset: JobsDataset, jm_files, wm_files,
                       jm_subset=None, wm_subset=None):

        jmdf = self.pick_subset(jm_dataset, jm_subset)
        wmdf = self.pick_subset(wm_dataset, wm_subset)

        jm_index = jmdf.index.name
        wm_index = wmdf.index.name

        jm_files = jm_files[jm_index.isin(jmdf.index)]
        wm_files = wm_files[wm_index.isin(wmdf.index)]

        possible_matches = self._all_file_matches(jm_files, wm_files, jm_index=jm_index, wm_index=wm_index)

        self.prefix_columns(jmdf, 'jmdf_')
        self.prefix_columns(wmdf, 'wmdf_')

        matched_jobs = possible_matches.join(jmdf, on=jm_index).join(wmdf, on=wm_index)
        filtered = self.filter_matches(matched_jobs, jm_dataset, wm_dataset, jmdf_prefix='jmdf_', wmdf_prefix='wmdf_')

        perfect_matches = filtered.groupby(jm_index).filter(lambda x: len(x) == 1)
        return perfect_matches[[jm_index, wm_index]]

    @staticmethod
    def pick_subset(dataset: JobsDataset, subset=None):
        return subset if subset is not None else dataset.jobs

    @staticmethod
    def prefix_columns(df, prefix=''):
        df.columns = map(lambda x: prefix + x, df.columns)

    @staticmethod
    def abs_timestamp_diff(ts_series1, ts_series2):
        return (ts_series1 - ts_series2).dt.total_seconds().abs()

    @staticmethod
    def group_by_time(df, ts_cols: List[str], freq):
        df = df.copy()
        for timestamp_col in ts_cols:
            df[timestamp_col] = df[timestamp_col].dt.floor(freq)

        print("Grouping by {}".format(ts_cols))
        return df.groupby(ts_cols)

    # TODO Refactor this to exclude the explicit index names.
    @staticmethod
    def _all_file_matches(jm_files, wm_files, file_col='FileName', jm_index='UniqueID', wm_index='wmaid'):

        matches = jm_files.merge(wm_files, on=file_col, how='inner')

        if matches.empty:
            return pd.DataFrame()
        else:
            return matches[[jm_index, wm_index]].drop_duplicates()