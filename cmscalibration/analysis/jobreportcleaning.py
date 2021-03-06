import logging

import numpy as np
import pandas as pd

from data.dataset import Metric


def clean_job_reports(df):
    df = _add_missing_walltimes(df)
    df = _add_missing_jobtypes(df)
    df = _core_thread_count_heuristic(df)
    df = _add_event_counts(df)

    return df


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

    grouped_dict = {Metric.WORKFLOW.value: '#unknown'}

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


def _add_missing_walltimes(df: pd.DataFrame):
    logging.debug("Filling in missing walltimes, missing before {}".format(df[Metric.WALL_TIME.value].isnull().sum()))

    df_filled = df.copy()

    from_timestamps = (df_filled[Metric.STOP_TIME.value] - df_filled[Metric.START_TIME.value]).dt.total_seconds()
    df_filled[Metric.WALL_TIME.value] = df_filled[Metric.WALL_TIME.value].fillna(from_timestamps)

    logging.debug(
        "Filling in missing walltimes, missing after {}".format(df_filled[Metric.WALL_TIME.value].isnull().sum()))

    return df_filled


def _add_event_counts(df: pd.DataFrame, fill_mean=True):
    df = df.copy()

    # If setup time is available, compute event counts from them
    events = ((df[Metric.WALL_TIME.value] - df[Metric.INIT_TIME.value]) * df[Metric.EVENT_THROUGHPUT.value]).round()

    logging.debug("Number of null events after filling in with init time: {}".format(events.isnull().sum()))

    # Fill in missing values without the setup time
    events = events.fillna(df[Metric.WALL_TIME.value] * df[Metric.EVENT_THROUGHPUT.value])

    logging.debug("Number of null events after filling in without init time: {}".format(events.isnull().sum()))

    # Fill in with nonzero event counts from JobMonitoring information

    events_nonzero_mask = df[Metric.EVENT_COUNT.value] > 0
    events.loc[events_nonzero_mask] = df.loc[events_nonzero_mask, Metric.EVENT_COUNT.value]

    logging.debug("Number of null events after filling in values from JobMonitoring: {}".format(events.isnull().sum()))

    df[Metric.EVENT_COUNT.value] = events

    if fill_mean:
        missing_events = df[Metric.EVENT_COUNT.value].isnull()

        grouped_dict = {Metric.JOB_CATEGORY.value: '#unknown',
                        Metric.JOB_TYPE.value: '#unknown',
                        Metric.EXIT_CODE.value: -1}

        df_filled = df.fillna(grouped_dict)

        median_filled_events = df_filled.groupby(list(grouped_dict.keys()))[Metric.EVENT_COUNT.value].transform(
            lambda x: x.fillna(x.median()))

        df.loc[missing_events, Metric.EVENT_COUNT.value] = median_filled_events.loc[missing_events]

        logging.debug("Number of null events after filling in mean event in group: {}".format(
            df[Metric.EVENT_COUNT.value].isnull().sum()))

    # Reset negative event counts
    df.loc[events < 0, Metric.EVENT_COUNT.value] = np.nan

    return df
