import logging

from data.dataset import Metric
from utils import stoex
from utils.histogram import bin_equal_width_overflow


def extract_job_demands(df, type_split_col=Metric.JOB_TYPE.value):
    """Extract resource demands from a data frame with job information.

    Returns a list of dictionaries containing a description of the statistical distribution of the
    resource demands in the form of Palladio stochastic expressions. It has the following keys:

    - typeName: The name of the job type, which is it type_split_col value.
    - cpuDemandStoEx: A stochastical expression describing the cpu demand distribution.
    - ioTimeStoEx: A stochastical expression describing the I/O time distribution needed by the jobs.
    - ioTimeRatioStoEx: A stochastical expression describing the distribution of the ratio of I/O time to CPU demand.
    - requiredJobslotsStoEx: A stochastical expression describing the distribution of needed job slots.
    - relativeFrequency: The relative frequency with which the job type occurs.

    :param df: A data frame containing job information. Requires performance information to be present in the data
    frame columns
    :param type_split_col: The column to be used to split the jobs into categories. Should be a categorical (or string)
    column. If a more complex split is needed, it should be added as an explicit column before calling this function.
    :return: A list of dictionaries as described above.
    """
    demands_list = []

    df_filtered = _filter_invalid_data(df)

    df_dict_by_type = split_by_column_value(df_filtered, type_split_col, copy=True)

    total_entries = df_filtered.shape[0]

    # Filter dictionary for rare job types
    df_types = {k: v for k, v in df_dict_by_type.items() if v.shape[0] / total_entries >= 0.001}
    logging.debug("Filtered data frames, dropped {} job types.".format(len(df_dict_by_type) - len(df_types)))

    filtered_entries = sum([df_type.shape[0] for key, df_type in df_types.items()])

    for name, jobs_of_type in df_types.items():
        demands_dict = {'typeName': name}

        logging.debug("Extracting CPU demand distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, 'CPUDemand')
        demands_dict['cpuDemandStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        logging.debug("Extracting I/O time distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, 'CPUIdleTime')
        demands_dict['ioTimeStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        logging.debug("Extracting I/O ratio distribution for job type {}.".format(name))
        counts, bins = extract_demand_distribution(jobs_of_type, 'CPUIdleTimeRatio')
        demands_dict['ioTimeRatioStoEx'] = stoex.hist_to_doublepdf(counts, bins)

        jobslots = extract_jobslot_distribution(jobs_of_type)
        demands_dict['requiredJobslotsStoEx'] = stoex.to_intpmf(jobslots.index, jobslots.values, simplify=True)

        # Compute the relative frequency of the job type
        relative_frequency = jobs_of_type.shape[0] / filtered_entries
        demands_dict['relativeFrequency'] = relative_frequency

        demands_list = demands_list + [demands_dict]

    return demands_list


def extract_demand_distribution(df, demand_col, bin_count=100):
    """Extract a histogram distribution from the provided column by creating an equal-width histogram."""

    counts, bins = bin_equal_width_overflow(df[demand_col], bin_count=bin_count, cutoff_quantile=0.95)
    return counts, bins


def extract_jobslot_distribution(df):
    """Extract the distribution of needed jobslots from a data frame."""

    return df[Metric.USED_CORES.value].value_counts().sort_index()


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
