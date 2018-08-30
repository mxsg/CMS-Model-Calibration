import logging

import numpy as np

from data.dataset import Metric
from utils import histogram


def add_performance_data(df):
    """Add performance information to a dataframe containing JobMonitoring job data."""

    job_data = df.copy()

    histogram.log_value_counts(job_data, Metric.WALL_TIME.value)
    histogram.log_value_counts(job_data, Metric.CPU_TIME.value)

    # Add the CPU demand for the job
    job_data[Metric.CPU_DEMAND.value] = job_data[Metric.CPU_TIME.value] * df[Metric.BENCHMARK_PER_THREAD.value]

    job_data[Metric.CPU_IDLE_TIME.value] = job_data[Metric.WALL_TIME.value] * job_data[Metric.USED_CORES.value] - \
                                           job_data[Metric.CPU_TIME.value]

    # Add I/O ratio via heuristic
    job_data[Metric.IO_RATIO.value] = job_data[Metric.CPU_IDLE_TIME.value] / job_data[Metric.CPU_DEMAND.value]
    job_data[Metric.IO_RATIO.value].replace([np.inf, -np.inf], np.nan, inplace=True)

    jobs_invalid_idle_time = job_data[job_data[Metric.CPU_IDLE_TIME.value] < 0]
    logging.debug("Number of jobs with invalid CPU idle time: {} (relative {})".format(jobs_invalid_idle_time.shape[0],
                                                                                       jobs_invalid_idle_time.shape[0] /
                                                                                       job_data.shape[0]))

    max_cpu_time = job_data[Metric.WALL_TIME.value] * job_data[Metric.USED_CORES.value]

    # Set invalid values of CPU idle time to NaN
    job_data.loc[(job_data[Metric.CPU_IDLE_TIME.value] < 0) | (
            job_data[Metric.CPU_IDLE_TIME.value] > max_cpu_time), Metric.CPU_IDLE_TIME.value] = np.nan

    job_data[Metric.CPU_IDLE_TIME_RATIO.value] = job_data[Metric.CPU_IDLE_TIME.value] / max_cpu_time.mask(
        max_cpu_time <= 0)

    job_data[Metric.CPU_EFFICIENCY.value] = job_data[Metric.CPU_TIME.value] / max_cpu_time.mask(max_cpu_time <= 0)

    # Reset invalid value for job CPU efficiency
    job_data.loc[(job_data[Metric.CPU_EFFICIENCY.value] < 0) | (
            job_data[Metric.CPU_EFFICIENCY.value] > 1), Metric.CPU_EFFICIENCY.value] = np.nan

    job_data[Metric.IO_TIME.value] = job_data[Metric.WRITE_TIME.value] + job_data[Metric.READ_TIME.value]

    # Add CPU Time per event
    job_data[Metric.CPU_TIME_PER_EVENT.value] = job_data[Metric.CPU_TIME.value] / job_data[Metric.EVENT_COUNT.value]
    job_data[Metric.CPU_DEMAND_PER_EVENT.value] = job_data[Metric.CPU_DEMAND.value] / job_data[Metric.EVENT_COUNT.value]

    # Replace infinite values with null values
    job_data[Metric.CPU_TIME_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)
    job_data[Metric.CPU_DEMAND_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)

    # Add CPU Idle time per event
    job_data[Metric.CPU_IDLE_TIME_PER_EVENT.value] = job_data[Metric.CPU_IDLE_TIME.value] / job_data[
        Metric.EVENT_COUNT.value]
    job_data[Metric.CPU_IDLE_TIME_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)

    histogram.log_value_counts(job_data, Metric.CPU_TIME.value)
    histogram.log_value_counts(job_data, Metric.EVENT_COUNT.value)

    histogram.log_value_counts(job_data, Metric.CPU_TIME_PER_EVENT.value)
    histogram.log_value_counts(job_data, Metric.CPU_DEMAND_PER_EVENT.value)

    return job_data


def add_missing_node_info(df, nodes):
    df_filled = df.copy()

    avg_rate_per_thread = nodes[Metric.BENCHMARK_PER_THREAD.value].mean()
    df_filled[Metric.BENCHMARK_PER_THREAD.value].fillna(avg_rate_per_thread, inplace=True)

    logging.debug("Average computing rate per thread: {}".format(avg_rate_per_thread))
    logging.debug("Average Benchmark total: {}".format(nodes[Metric.BENCHMARK_TOTAL.value].mean()))
    logging.debug("Average number of jobslots: {}".format(nodes[Metric.JOBSLOT_COUNT.value].mean()))

    return df_filled


def compute_average_cpu_efficiency(df, start=None, end=None):
    if start is not None:
        df = df[df['Timestamp'] >= start]

    if end is not None:
        df = df[df['Timestamp'] >= end]

    return df[Metric.CPU_EFFICIENCY.value].mean()
