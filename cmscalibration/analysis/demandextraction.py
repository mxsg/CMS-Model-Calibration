import logging
from abc import abstractmethod, ABCMeta
from typing import Dict, Optional

import pandas as pd

from data.dataset import Metric
from utils import stoex, visualization
from utils.histogram import bin_equal_width_overflow, bin_by_quantile
from utils.visualization import MultiPlotFigure


def split_group_by_median(job_groups, group_key, col):
    split_group = job_groups[group_key]
    median_value = split_group[col].median()

    group_low = split_group[split_group[Metric.CPU_IDLE_TIME_RATIO.value] <= median_value]
    group_high = split_group[split_group[Metric.CPU_IDLE_TIME_RATIO.value] > median_value]

    logging.debug("Splitting group with {} entries, lower {}, upper {} ({} missing).".format(split_group.shape[0],
                                                                                             group_low.shape[0],
                                                                                             group_high.shape[0],
                                                                                             split_group.shape[0] -
                                                                                             group_low.shape[0] -
                                                                                             group_high.shape[0]))

    del job_groups[group_key]

    job_groups['{}lower'.format(group_key)] = group_low
    job_groups['{}upper'.format(group_key)] = group_high


def split_group_by_value(job_groups, group_key, col, value=None):
    split_group = job_groups[group_key]

    if not value:
        value = split_group[col].median()

    group_low = split_group[split_group[col] <= value]
    group_high = split_group[split_group[col] > value]

    logging.debug("Splitting group with {} entries, lower {}, upper {} ({} missing).".format(split_group.shape[0],
                                                                                             group_low.shape[0],
                                                                                             group_high.shape[0],
                                                                                             split_group.shape[0] -
                                                                                             group_low.shape[0] -
                                                                                             group_high.shape[0]))

    del job_groups[group_key]

    job_groups['{}lower'.format(group_key)] = group_low
    job_groups['{}upper'.format(group_key)] = group_high


class JobDemandExtractor:

    def __init__(self,
                 report,
                 equal_width=True,
                 drop_overflow=False,
                 bin_count=100,
                 cutoff_quantile=0.95,
                 overflow_agg='median'):
        self.report = report
        self.equal_width = equal_width
        self.drop_overflow = drop_overflow
        self.bin_count = bin_count
        self.cutoff_quantile = cutoff_quantile
        self.overflow_agg = overflow_agg

    def extract_job_demands(self, df_types, type_share_summary=None):
        """Extract resource demands from a data frame with job information.

        Returns a list of dictionaries containing a description of the statistical distribution of the
        resource demands in the form of Palladio stochastic expressions. It has the following keys:

        - typeName: The name of the job type, which is it type_split_col value.
        - cpuDemandStoEx: A stochastical expression describing the cpu demand distribution.
        - ioTimeStoEx: A stochastical expression describing the I/O time distribution needed by the jobs.
        - ioTimeRatioStoEx: A stochastical expression describing the distribution of the ratio of I/O time to
        CPU demand
        - requiredJobslotsStoEx: A stochastical expression describing the distribution of needed job slots.
        - relativeFrequency: The relative frequency with which the job type occurs.
        - eventCountStoEx: The number of events.
        - cpuDemandPerEventStoEx: The CPU demand per event.
        - ioTimePerEvent: The I/O time per event.

        :param df_types: A dictionary containing groups of jobs to be used for resource demand extraction. Requires
        performance information to be present in the data frame columns.
        :param type_share_summary: A summary description of the shares of different types of jobs as grouped in
        df_types.
        :return: A list of dictionaries as described above.
        """

        demands_list = []

        self.report.append("## Resource Demand Extraction")

        filtered_entries = sum([df_type.shape[0] for key, df_type in df_types.items()])

        plot_count = len(df_types)
        ncols = 2

        jobslot_fig = MultiPlotFigure(nplots=plot_count, ncols=ncols)
        cpu_fig = MultiPlotFigure(nplots=plot_count, ncols=ncols)
        io_demand_fig = MultiPlotFigure(nplots=plot_count, ncols=ncols)
        io_ratio_fig = MultiPlotFigure(nplots=plot_count, ncols=ncols)

        job_types = [tuple(x) for x in df_types.items()]
        job_types.sort(key=lambda x: x[1].shape[0], reverse=True)

        # Write job types to report
        self.report.append("Job categories used for analysis:")
        self.report.append()
        for job_identifier, jobs in job_types:
            self.report.append("- {}: {} reports".format(job_identifier, jobs.shape[0]))
        self.report.append()

        for name, jobs_of_type in job_types:
            demands_dict = {'typeName': name}

            # CPU Demands

            logging.debug("Extracting CPU demand distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.CPU_DEMAND.value)

            self.create_figures(counts, bins, name,
                                r"CPU Demand / (s $\cdot$ HS06 per core)",
                                "CPU demand distribution",
                                "cpu_demands",
                                cpu_fig)

            demands_dict['cpuDemandStoEx'] = stoex.hist_to_doublepdf(counts, bins)

            # I/O Time

            logging.debug("Extracting I/O time distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.CPU_IDLE_TIME.value)

            self.create_figures(counts, bins, name,
                                "Estimated I/O time / s",
                                "I/O time distribution",
                                "io_time",
                                io_demand_fig)

            demands_dict['ioTimeStoEx'] = stoex.hist_to_doublepdf(counts, bins)

            # I/O Ratio

            logging.debug("Extracting I/O ratio distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.IO_RATIO.value)

            self.create_figures(counts, bins, name,
                                "I/O time ratio of CPU demand",
                                "I/O ratio distribution",
                                "io_ratio",
                                io_ratio_fig)

            demands_dict['ioTimeRatioStoEx'] = stoex.hist_to_doublepdf(counts, bins)

            self.report.append("### Demands per Event")
            self.report.append()

            # Event Counts

            logging.debug("Extracting event count distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.EVENT_COUNT.value)

            self.create_figures(counts, bins, name,
                                "Number of processed events",
                                "Event number distribution",
                                "event_counts",
                                None)

            demands_dict['eventCountStoEx'] = stoex.hist_to_doublepdf(counts, bins)

            # CPU Demand per Event

            logging.debug("Extracting CPU demand per Event distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.CPU_DEMAND_PER_EVENT.value)

            self.create_figures(counts, bins, name,
                                "CPU demand per event / (s $\cdot$ HS06 per core)",
                                "CPU demand distribution per event",
                                "cpu_demand_per_event",
                                None)

            demands_dict['cpuDemandPerEventStoEx'] = stoex.hist_to_doublepdf(counts, bins)

            # I/O Time Per Event

            logging.debug("Extracting I/O time per Event distribution for job type {}.".format(name))
            counts, bins = self.extract_demand_distribution(jobs_of_type, Metric.CPU_IDLE_TIME_PER_EVENT.value)

            self.create_figures(counts, bins, name,
                                "I/O time per event / s",
                                "I/O time distribution per per event",
                                "io_time_per_event",
                                None)

            demands_dict['ioTimePerEvent'] = stoex.hist_to_doublepdf(counts, bins)

            # Job slots

            jobslots = self.extract_jobslot_distribution(jobs_of_type)
            demands_dict['requiredJobslotsStoEx'] = stoex.to_intpmf(jobslots.index, jobslots.values, simplify=True)

            jobslot_bins = range(1, 9)
            jobslot_counts = [0] * len(jobslot_bins)

            for i, slots in enumerate(jobslot_bins):
                if slots in jobslots.index:
                    jobslot_counts[i] = jobslots.loc[slots]

            fig, axes = visualization.draw_integer_distribution(jobslot_bins, jobslot_counts, name=name)
            visualization.draw_integer_distribution_subplot(jobslot_bins, jobslot_counts,
                                                            jobslot_fig.current_axis, name=name)
            jobslot_fig.finish_subplot()

            self.report.add_figure(fig, axes, 'jobslots_type_{}'.format(name))

            # Job Groupe Shares

            if type_share_summary is None:
                # Compute the relative frequency of the job type
                relative_frequency = jobs_of_type.shape[0] / filtered_entries
                demands_dict['relativeFrequency'] = relative_frequency
            else:
                # Normalize shares from type share summary
                type_share_sum = sum(type_share_summary[type_name] for type_name in df_types.keys())
                demands_dict['relativeFrequency'] = type_share_summary[name] / type_share_sum

            demands_list.append(demands_dict)

        # Add overview figures to the report
        cpu_fig.add_to_report(self.report, 'cpu_demand_overview')
        io_demand_fig.add_to_report(self.report, 'io_demand_overview')
        io_ratio_fig.add_to_report(self.report, 'io_ratio_overview')
        jobslot_fig.add_to_report(self.report, 'jobslots_overview')

        return demands_list, df_types

    def extract_demand_distribution(self, df, demand_col):
        """Extract a histogram distribution from the provided column by creating an equal-width histogram."""

        x = df[demand_col].copy()

        # Filter negative and null values
        x = x.dropna()
        x = x[x >= 0.0]

        if self.equal_width:
            counts, bins = bin_equal_width_overflow(x, bin_count=self.bin_count, cutoff_quantile=self.cutoff_quantile)
        else:
            counts, bins = bin_by_quantile(x, bin_count=self.bin_count, cutoff_quantile=self.cutoff_quantile,
                                           drop_overflow=self.drop_overflow, overflow_agg=self.overflow_agg)

        return counts, bins

    @staticmethod
    def extract_jobslot_distribution(df):
        """Extract the distribution of needed jobslots from a data frame."""
        return df[Metric.USED_CORES.value].astype(int).value_counts().sort_index()

    def create_figures(self, counts, bins, type_name, xlabel, plot_title, plot_identifier,
                       overview_figure: Optional[MultiPlotFigure]):

        ylabel = "Probability Density"

        fig, axes = visualization.draw_binned_data(counts, bins)

        axes.set_xlabel(xlabel)
        axes.set_ylabel(ylabel)

        axes.set_title("{} for jobs of type: {}".format(plot_title, type_name))

        self.report.add_figure(fig, axes, '{}_type_{}'.format(plot_identifier, type_name))

        if overview_figure:
            current_axis = overview_figure.current_axis
            visualization.draw_binned_data_subplot(counts, bins, current_axis, name=type_name)

            current_axis.set_ylabel(xlabel)
            current_axis.set_xlabel(ylabel)
            overview_figure.finish_subplot()


class AbstractJobClassifier(metaclass=ABCMeta):

    @abstractmethod
    def split(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return NotImplemented


class ColumnJobClassifier(AbstractJobClassifier):

    def __init__(self, colname: str, copy=False):
        self.colname = colname
        self.copy = copy

    def split(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        logging.debug("Splitting data frame by column.")
        values = df[self.colname].unique()

        if self.copy:
            partitions = {value: df[df[self.colname] == value].copy() for value in values}
        else:
            partitions = {value: df[df[self.colname] == value] for value in values}

        return partitions


class ColumnListJobClassifier(AbstractJobClassifier):

    def __init__(self, cols):
        self.cols = cols

    def split(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        logging.debug("Splitting data frame by columns: {}".format(self.cols))

        grouped = df.groupby(self.cols)

        partitions = {}

        for key, group in grouped:
            string_keys = map(str, key)
            group_key = "".join(string_keys)
            partitions[group_key] = group.copy()

        return partitions


class FilteredJobClassifier(AbstractJobClassifier):

    def __init__(self, type_split_cols=None, min_rel_freq=0.0005, split_types=None):
        self.type_split_cols = type_split_cols
        if not type_split_cols:
            self.type_split_cols = [Metric.JOB_TYPE.value]

        self.min_rel_freq = min_rel_freq

        # split_types is of structure: [(group_key, column, value)]
        self.split_types = split_types

    def split(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df_filtered = self._filter_invalid_data(df)

        total_entries = df_filtered.shape[0]

        df_dict_by_type = ColumnListJobClassifier(self.type_split_cols).split(df_filtered)

        # Filter dictionary for rare job types
        df_types = {k: v for k, v in df_dict_by_type.items() if v.shape[0] / total_entries >= self.min_rel_freq}
        logging.debug("Filtered data frames, dropped {} job types.".format(len(df_dict_by_type) - len(df_types)))

        if self.split_types is not None:
            for group_key, split_col, value in self.split_types:
                self.split_group_by_value(df_types, group_key, split_col, value)

        logging.debug("Kept group types: {}".format(list(df_types.keys())))

        return df_types

    @staticmethod
    def _filter_invalid_data(df):
        logging.debug("Number of entries before: {}".format(df.shape[0]))

        # Remove data with zero walltime but nonzero CPU time
        df_subset = df.drop(df[(df[Metric.WALL_TIME.value] > 0) & (df[Metric.CPU_TIME.value] <= 0)].index)

        # Remove data with zero CPU time but non-zero Walltime
        df_subset = df_subset.drop(
            df_subset[(df_subset[Metric.CPU_TIME.value] <= 0) & (df_subset[Metric.WALL_TIME.value] > 0)].index)

        logging.debug("Number of entries after: {}".format(df_subset.shape[0]))

        return df_subset

    @staticmethod
    def split_group_by_value(job_groups, group_key, col, value=None):
        split_group = job_groups[group_key]

        if not value:
            value = split_group[col].median()

        group_low = split_group[split_group[col] <= value]
        group_high = split_group[split_group[col] > value]

        logging.debug("Splitting group with {} entries, lower {}, upper {} ({} missing).".format(split_group.shape[0],
                                                                                                 group_low.shape[0],
                                                                                                 group_high.shape[0],
                                                                                                 split_group.shape[0] -
                                                                                                 group_low.shape[0] -
                                                                                                 group_high.shape[0]))

        del job_groups[group_key]

        job_groups['{}lower'.format(group_key)] = group_low
        job_groups['{}upper'.format(group_key)] = group_high
