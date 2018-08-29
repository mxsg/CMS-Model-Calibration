import logging
import math
from abc import abstractmethod, ABCMeta
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd

import utils.report as rp
from data.dataset import Metric
from utils import stoex, visualization
from utils.histogram import bin_equal_width_overflow, bin_by_quantile


def split_and_filter(df, type_split_cols=None):
    if not type_split_cols:
        type_split_cols = [Metric.JOB_TYPE.value]

    df_filtered = _filter_invalid_data(df)

    total_entries = df_filtered.shape[0]

    df_dict_by_type = split_by_column_list(df_filtered, type_split_cols)

    # Filter dictionary for rare job types
    df_types = {k: v for k, v in df_dict_by_type.items() if v.shape[0] / total_entries >= 0.0005}
    logging.debug("Filtered data frames, dropped {} job types.".format(len(df_dict_by_type) - len(df_types)))
    logging.debug("Kept group types: {}".format(list(df_types.keys())))

    return df_types


def split_group_by_median(job_groups, group_key, col):
    split_group = job_groups[group_key]
    median_value = split_group[col].median()

    group_low = split_group[split_group[Metric.CPU_IDLE_TIME_RATIO.value] <= median_value]
    group_high = split_group[split_group[Metric.CPU_IDLE_TIME_RATIO.value] > median_value]
    # group_high = split_group.drop(split_group.index)

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
    # group_high = split_group.drop(split_group.index)

    logging.debug("Splitting group with {} entries, lower {}, upper {} ({} missing).".format(split_group.shape[0],
                                                                                             group_low.shape[0],
                                                                                             group_high.shape[0],
                                                                                             split_group.shape[0] -
                                                                                             group_low.shape[0] -
                                                                                             group_high.shape[0]))

    del job_groups[group_key]

    job_groups['{}lower'.format(group_key)] = group_low
    job_groups['{}upper'.format(group_key)] = group_high


def extract_job_demands(df_types: Dict[str, pd.DataFrame], report: rp.ReportBuilder,
                        type_share_summary=None,
                        equal_width=True, drop_overflow=False, bin_count=100, cutoff_quantile=0.95, overflow_agg='median'):
    """Extract resource demands from a data frame with job information.

    Returns a list of dictionaries containing a description of the statistical distribution of the
    resource demands in the form of Palladio stochastic expressions. It has the following keys:

    - typeName: The name of the job type, which is it type_split_col value.
    - cpuDemandStoEx: A stochastical expression describing the cpu demand distribution.
    - ioTimeStoEx: A stochastical expression describing the I/O time distribution needed by the jobs.
    - ioTimeRatioStoEx: A stochastical expression describing the distribution of the ratio of I/O time to CPU demand.
    - requiredJobslotsStoEx: A stochastical expression describing the distribution of needed job slots.
    - relativeFrequency: The relative frequency with which the job type occurs.

    :param df_types: A dictionary containing groups of jobs to be used for resource demand extraction. Requires performance
    information to be present in the data
    frame columns
    :param type_split_cols: The column to be used to split the jobs into categories. Should be a categorical (or string)
    column. If a more complex split is needed, it should be added as an explicit column before calling this function.
    :return: A list of dictionaries as described above.
    """
    demands_list = []

    report.append("## Resource Demand Extraction")

    filtered_entries = sum([df_type.shape[0] for key, df_type in df_types.items()])

    # Todo Refactor this!
    # Setup subplots
    nplots = len(df_types)
    ncols = 2
    nrows = math.ceil(nplots / ncols)

    jobslot_fig, jobslot_axes = plt.subplots(ncols=ncols, nrows=nrows)
    cpu_fig, cpu_axes = plt.subplots(ncols=ncols, nrows=nrows)
    io_fig, io_axes = plt.subplots(ncols=ncols, nrows=nrows)
    io_dem_fig, io_dem_axes = plt.subplots(ncols=ncols, nrows=nrows)

    i_subplot = 0

    job_types = [tuple(x) for x in df_types.items()]
    job_types.sort(key=lambda x: x[1].shape[0], reverse=True)

    # Write to report
    report.append("Job categories used for analysis:")
    report.append()
    for type, jobs in job_types:
        report.append("- {}: {} reports".format(type, jobs.shape[0]))
    report.append()

    for name, jobs_of_type in job_types:
        demands_dict = {'typeName': name}

        logging.debug("Extracting CPU demand distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.CPU_DEMAND.value, equal_width=equal_width,
                                                   drop_overflow=drop_overflow, bin_count=bin_count,
                                                   cutoff_quantile=cutoff_quantile, overflow_agg=overflow_agg)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"CPU Demand / (s $\cdot$ (HS06 Score per Core))")
        axes.set_ylabel("Probability Density")
        axes.set_title("CPU Demand Distribution for Jobs of Type: {}".format(name))

        report.add_figure(fig, axes, 'cpu_demands_type_{}'.format(name))

        cpu_axis = cpu_axes[i_subplot // ncols, i_subplot % ncols]
        visualization.draw_binned_data_subplot(counts, bins, cpu_axis, name=name)

        cpu_axis.set_xlabel(r"CPU Demand / (s $\cdot$ HS06 per core)")
        cpu_axis.set_ylabel("Probability Density")

        demands_dict['cpuDemandStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        logging.debug("Extracting I/O time distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.CPU_IDLE_TIME.value, equal_width=equal_width,
                                                   drop_overflow=drop_overflow, bin_count=bin_count,
                                                   cutoff_quantile=cutoff_quantile, overflow_agg=overflow_agg)
        demands_dict['ioTimeStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"Estimated I/O Time / s")
        axes.set_ylabel("Probability Density")
        axes.set_title("I/O time distribution for jobs of type: {}".format(name))

        report.add_figure(fig, axes, 'io_time_type_{}'.format(name))

        io_dem_axis = io_dem_axes[i_subplot // ncols, i_subplot % ncols]
        visualization.draw_binned_data_subplot(counts, bins, io_dem_axis, name=name)

        io_dem_axis.set_xlabel(r"Estimated I/O Time / s")
        io_dem_axis.set_ylabel("Probability Density")

        logging.debug("Extracting I/O ratio distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.IO_RATIO.value, equal_width=equal_width,
                                                   drop_overflow=drop_overflow, bin_count=bin_count,
                                                   cutoff_quantile=cutoff_quantile, overflow_agg=overflow_agg)
        demands_dict['ioTimeRatioStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"I/O Time Ratio of CPU Demand")
        axes.set_ylabel("Probability Density")
        axes.set_title("I/O Ratio Distribution for Jobs of Type: {}".format(name))

        report.add_figure(fig, axes, 'io_ratio_type_{}'.format(name))

        io_axis = io_axes[i_subplot // ncols, i_subplot % ncols]
        visualization.draw_binned_data_subplot(counts, bins, io_axis, name=name)

        io_axis.set_xlabel(r"I/O Ratio of CPU Demand")
        io_axis.set_ylabel("Probability Density")

        # Add overview of event counts

        logging.debug("Extracting Event count distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.EVENT_COUNT.value, equal_width=False,
                                                   drop_overflow=False, cutoff_quantile=cutoff_quantile,
                                                   bin_count=bin_count, overflow_agg=overflow_agg)

        demands_dict['eventCountStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"Number of Events")
        axes.set_ylabel("Probability Density")
        axes.set_title("Event number distribution for jobs of type: {}".format(name))

        report.add_figure(fig, axes, 'event_counts_type_{}'.format(name))

        # Add overview over the CPU demand per event

        logging.debug("Extracting Event count distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.CPU_DEMAND_PER_EVENT.value, equal_width=False,
                                                   drop_overflow=False, bin_count=bin_count,
                                                   cutoff_quantile=cutoff_quantile, overflow_agg=overflow_agg)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"CPU Demand per Event")
        axes.set_ylabel("Probability Density")
        axes.set_title("CPU Demand distribution per event for jobs of type: {}".format(name))

        report.add_figure(fig, axes, 'cpu_demand_per_event_type_{}'.format(name))

        demands_dict['cpuDemandPerEventStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        logging.debug("Extracting I/O time distribution per event for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, Metric.CPU_IDLE_TIME_PER_EVENT.value,
                                                   equal_width=False,
                                                   drop_overflow=False, bin_count=bin_count,
                                                   cutoff_quantile=cutoff_quantile, overflow_agg=overflow_agg)

        fig, axes = visualization.draw_binned_data(counts, bins)
        axes.set_xlabel(r"I/O time per Event")
        axes.set_ylabel("Probability Density")
        axes.set_title("I/O time distribution per event for jobs of type: {}".format(name))

        report.add_figure(fig, axes, 'io_time_per_event_type_{}'.format(name))

        demands_dict['ioTimePerEvent'] = stoex.hist_to_doublepdf(counts, bins)

        jobslots = extract_jobslot_distribution(jobs_of_type)
        demands_dict['requiredJobslotsStoEx'] = stoex.to_intpmf(jobslots.index, jobslots.values, simplify=True)

        jobslot_bins = range(1, 9)
        jobslot_counts = [0] * len(jobslot_bins)

        for i, slots in enumerate(jobslot_bins):
            if slots in jobslots.index:
                jobslot_counts[i] = jobslots.loc[slots]

        fig, axes = visualization.draw_integer_distribution(jobslot_bins, jobslot_counts, name=name)
        visualization.draw_integer_distribution_subplot(jobslot_bins, jobslot_counts,
                                                        jobslot_axes[i_subplot // ncols, i_subplot % ncols], name=name)

        # axes.set_title("Job Slot Distribution for jobs of type: {}".format(name))

        report.add_figure(fig, axes, 'jobslots_type_{}'.format(name))

        if type_share_summary is None:
            # Compute the relative frequency of the job type
            relative_frequency = jobs_of_type.shape[0] / filtered_entries
            demands_dict['relativeFrequency'] = relative_frequency
        else:
            type_share_summary = type_share_summary.set_index('type')
            type_shares = [type_share_summary.loc[key]['share'] for key in df_types.keys()]

            # Normalize shares again in case it changed …
            type_share_sum = sum(share for _, share in df_types.items())
            for key, share in type_shares.items():
                type_shares[key] = share / type_share_sum

            # Todo Fix this!
            # relative_frequency = type_share_summary.loc

        demands_list = demands_list + [demands_dict]

        i_subplot += 1

    def configure_overview_plot(fig, axes, identifier):
        fig.set_size_inches(14, 12)

        # Remove last plot if odd number of plots is encountered
        if nplots % ncols != 0:
            fig.delaxes(axes[nplots // ncols, nplots % 2])

        # Add overview figure to the report
        report.add_figure(fig, axes, identifier)

    # Add overview figures to the report
    configure_overview_plot(jobslot_fig, jobslot_axes, 'jobslots_overview')
    configure_overview_plot(cpu_fig, cpu_axes, 'cpu_demand_overview')
    configure_overview_plot(io_fig, io_axes, 'io_ratio_overview')
    configure_overview_plot(io_dem_fig, io_dem_axes, 'io_demand_overview')

    return demands_list, df_types


def extract_demand_distribution(df, demand_col, bin_count=100, equal_width=True, drop_overflow=False,
                                cutoff_quantile=0.95, overflow_agg='median'):
    """Extract a histogram distribution from the provided column by creating an equal-width histogram."""

    x = df[demand_col].copy()

    # Filter negative and null values
    x = x.dropna()
    x = x[x >= 0.0]

    if equal_width:
        counts, bins = bin_equal_width_overflow(x, bin_count=bin_count, cutoff_quantile=cutoff_quantile)
    else:
        counts, bins = bin_by_quantile(x, bin_count=bin_count, cutoff_quantile=cutoff_quantile,
                                       drop_overflow=drop_overflow, overflow_agg=overflow_agg)
        # Todo reset overflow aggregation function to median again?

    return counts, bins


def extract_jobslot_distribution(df):
    """Extract the distribution of needed jobslots from a data frame."""
    return df[Metric.USED_CORES.value].astype(int).value_counts().sort_index()


def _filter_invalid_data(df):
    logging.debug("Number of entries before: {}".format(df.shape[0]))

    # Remove data with zero walltime but nonzero CPU time
    df_subset = df.drop(df[(df[Metric.WALL_TIME.value] > 0) & (df[Metric.CPU_TIME.value] <= 0)].index)

    # Remove data with zero CPU time but non-zero Walltime
    df_subset = df_subset.drop(
        df_subset[(df_subset[Metric.CPU_TIME.value] <= 0) & (df_subset[Metric.WALL_TIME.value] > 0)].index)

    logging.debug("Number of entries after: {}".format(df_subset.shape[0]))

    return df_subset


def _filter_df_by_type(df, min_rel_freq):
    filtered_frequencies = _filter_job_frequencies(df, min_rel_freq)
    filtered_df = df.loc[df[Metric.JOB_TYPE.value].isin(filtered_frequencies.index)].copy()

    return filtered_df, filtered_frequencies


def _filter_job_frequencies(df, min_rel_freq):
    rel_frequencies = df[Metric.JOB_TYPE.value].value_counts() / len(df)

    filtered_frequencies = rel_frequencies[rel_frequencies < min_rel_freq]

    # Normalize after dropping
    filtered_frequencies = filtered_frequencies / sum(filtered_frequencies)
    return filtered_frequencies


def split_by_column_value(df, colname, copy=False):
    """ Split a data frame into partitions based on the value of a column.

    :param df: The data frame to split
    :param colname: The name of the column to split by.
    :param copy: If true, return copies of the data frames.
    :return: A dictionary containing the column value as keys and the data frames belonging to
    the value as values.
    """
    logging.debug("Splitting data frame by column.")
    values = df[colname].unique()

    if copy:
        partitions = {value: df[df[colname] == value].copy() for value in values}
    else:
        partitions = {value: df[df[colname] == value] for value in values}

    return partitions


# Todo Make this more general
# Todo Make groups with null values? (rather not, as they would not contain useful data)
def split_by_column_combination(df, primary_col, secondary_col):
    logging.debug("Splitting data frame by columns: primary {}, secondary {}.".format(primary_col, secondary_col))

    primary_values = df[primary_col].unique()

    def make_group_key(col1, val1, col2, val2):
        return "{}{}".format(val1, val2)

    partitions = {}
    for primary_value in primary_values:
        primary_group = df[df[primary_col] == primary_value]
        secondary_values = primary_group[secondary_col].unique()

        new_groups = {make_group_key(primary_col, primary_value, secondary_col, secondary_value): primary_group[
            primary_group[secondary_col] == secondary_value].copy()
                      for secondary_value in secondary_values}
        partitions.update(new_groups)

    return partitions


def split_by_column_list(df, cols):
    logging.debug("Splitting data frame by columns: {}".format(cols))

    grouped = df.groupby(cols)

    partitions = {}

    for key, group in grouped:
        string_keys = map(str, key)
        group_key = "".join(string_keys)
        partitions[group_key] = group.copy()

    return partitions


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
