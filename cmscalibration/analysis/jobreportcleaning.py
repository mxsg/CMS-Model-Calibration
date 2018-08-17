import logging

import pandas as pd

from data.dataset import Metric


def clean_job_reports(df):
    df = _add_missing_walltimes(df)
    df = _add_missing_jobtypes(df)
    df = _core_thread_count_heuristic(df)
    # df = _add_missing_core_counts(df)
    return df


def _add_missing_walltimes(df):
    df = df.copy()
    df[Metric.WALL_TIME.value] = df[Metric.WALL_TIME.value].fillna(
        (df[Metric.STOP_TIME.value] - df[Metric.START_TIME.value]).dt.total_seconds())
    return df


def _add_missing_core_counts(df):
    df_filled = df.copy()

    logging.debug("Filling in missing core count information, missing before {}".format(
        df_filled[Metric.USED_CORES.value].isnull().sum()))

    # TODO Improve on this: This only fills values for workflows where the number of cores is unique across the data set
    def fill_unique(series):
        if series[series > 0].nunique() == 1:
            unique_value = series.loc[series.first_valid_index()]
            return unique_value
        else:
            return series

    grouped_metrics = [Metric.WORKFLOW.value, Metric.JOB_CATEGORY.value]
    # grouped_metrics = [Metric.WORKFLOW.value]

    df_filled[Metric.USED_CORES.value] = df_filled.groupby(grouped_metrics)[Metric.USED_CORES.value].transform(
        fill_unique)

    # # Todo Enable filling of used core counts
    # df_filled[Metric.USED_CORES.value] = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].transform(
    #     lambda x: x.fillna(x.median()))

    # Core counts must be positive, so we can use negative value to fill misssing values
    # df_filled[Metric.USED_CORES.value] = df_filled[Metric.USED_CORES.value].fillna(-1)

    # Find workflows with only one unique non-null core count
    # Use unfilled data frame for this!

    # def fill_unique(group):
    #     if group[group > 0].nunique() == 1:
    #         unique_value = group[group > 0].unique()[0]
    #         group[group < 0] = unique_value
    #
    #     return group

    # single_core_count = df.groupby(Metric.WORKFLOW.value).filter(
    #     lambda x: x[Metric.USED_CORES.value].nunique() == 1)

    # df_filled[Metric.USED_CORES.value] = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].apply(
    #     fill_unique)

    # Fill values in filtered data frame
    # df[Metric.USED_CORES.value] = single_core_count.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].apply(
    #     lambda x: x.ffill().bfill())

    # core_counts_per_workflow = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].nunique()

    # Reset filled value again
    # df_filled.loc[df_filled[Metric.USED_CORES.value] < 0, Metric.USED_CORES.value] = np.NaN

    logging.debug("Filling in missing core count information, missing after {}".format(
        df_filled[Metric.USED_CORES.value].isnull().sum()))

    return df_filled


def _core_thread_count_heuristic(df: pd.DataFrame):
    df = df.copy()

    # Find job categories that are always single-threaded
    # Note: This might change in the future or in other data sets

    singlethreaded_categories = ['harvesting', 'logcollect', 'merge']
    mask = df[Metric.JOB_CATEGORY.value].isin(singlethreaded_categories)

    df.loc[mask, Metric.USED_CORES.value] = 1
    df.loc[mask, Metric.USED_THREADS.value] = 1

    # Fill in jobs with number of used cores of 1 and 2, which are identical in their
    # numbers of threads and streams
    direct_fill = [1.0, 2.0]

    # Find out position where either of the metrics is set to the directly filled value
    masks = {value: (df[Metric.USED_CORES.value] == value) | (df[Metric.USED_THREADS.value] == value) for value in
             direct_fill}

    # Now set appropriate values
    for value, mask in masks.items():
        df.loc[mask, Metric.USED_CORES.value] = value
        df.loc[mask, Metric.USED_THREADS.value] = value

    df = _advanced_heuristics_thread_count(df)

    return df


# Todo Be careful, these might be wrong!
def _advanced_heuristics_thread_count(df: pd.DataFrame):
    logging.debug("Filling in missing core count information (same thread + core count), missing before {}".format(
        df[Metric.USED_CORES.value].isnull().sum()))

    df = df.copy()

    # In most cases, thread and core counts are identical
    replace_cores = df[Metric.USED_CORES.value].isnull() & df[Metric.USED_THREADS.value].notnull()
    replace_threads = df[Metric.USED_CORES.value].notnull() & df[Metric.USED_THREADS.value].isnull()

    df.loc[replace_cores, Metric.USED_CORES.value] = df.loc[replace_cores, Metric.USED_THREADS.value]
    df.loc[replace_threads, Metric.USED_THREADS.value] = df.loc[replace_threads, Metric.USED_CORES.value]

    logging.debug("Filling in missing core count information (same thread + core count), missing after {}".format(
        df[Metric.USED_CORES.value].isnull().sum()))

    # Filling in missing counts with median from the group
    both_missing = df[Metric.USED_CORES.value].isnull() & df[Metric.USED_THREADS.value].isnull()

    # grouped_dict = {Metric.WORKFLOW.value: '#unknown', Metric.JOB_CATEGORY.value: '#unknown',
    #                 Metric.JOB_TYPE.value: '#unknown'}

    grouped_dict = {Metric.JOB_CATEGORY.value: '#unknown',
                    Metric.JOB_TYPE.value: '#unknown'}

    df_filled = df.fillna(grouped_dict)
    median_filled_cores = df_filled.groupby(list(grouped_dict.keys()))[Metric.USED_CORES.value].transform(
        lambda x: x.fillna(x.median()))

    df.loc[both_missing, Metric.USED_CORES.value] = median_filled_cores.loc[both_missing]

    logging.debug("Filling in missing core count information (fill median), missing after {}".format(
        df[Metric.USED_CORES.value].isnull().sum()))

    return df


def _add_missing_jobtypes(df: pd.DataFrame):
    logging.debug("Filling in missing job types, missing before {}".format(df[Metric.JOB_TYPE.value].isnull().sum()))

    df_filled = df.copy()

    def fill_unique(series):
        if series.nunique() == 1:
            unique_value = series.loc[series.first_valid_index()]
            return unique_value
        else:
            return series

    df_filled[Metric.JOB_TYPE.value] = df_filled.groupby(Metric.WORKFLOW.value)[Metric.JOB_TYPE.value].transform(
        fill_unique)

    logging.debug(
        "Missing after filling in missing job types: {}".format(df_filled[Metric.JOB_TYPE.value].isnull().sum()))

    return df_filled
