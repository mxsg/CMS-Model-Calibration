import logging

import numpy as np

from data.dataset import Metric
from utils import histogram


def add_performance_data(df):
    """Add performance information to a dataframe containing JobMonitoring job data."""

    wrap_cpu = Metric.CPU_TIME.value
    wrap_wc = Metric.WALL_TIME.value
    job_type = Metric.JOB_TYPE.value
    core_count = Metric.USED_CORES.value

    cpu_idle_time = Metric.CPU_IDLE_TIME.value
    job_cpu_efficiency = Metric.OVERALL_CPU_EFFICIENCY.value
    cpu_idle_time_ratio = Metric.CPU_IDLE_TIME_RATIO.value

    job_data = df.copy()

    logging.debug("WrapCPU summary: positive {}, zero {}, negative {}".format(job_data[job_data[wrap_cpu] > 0].shape[0],
                                                                              job_data[job_data[wrap_cpu] == 0].shape[
                                                                                  0],
                                                                              job_data[job_data[wrap_cpu] < 0].shape[
                                                                                  0]))

    logging.debug("WrapWC summary: positive {}, zero {}, negative {}".format(job_data[job_data[wrap_wc] > 0].shape[0],
                                                                             job_data[job_data[wrap_wc] == 0].shape[0],
                                                                             job_data[job_data[wrap_wc] < 0].shape[0]))

    logging.debug("NCores summary:\n" + job_data[core_count].value_counts().to_string())
    logging.debug("Types summary:\n" + job_data[job_type].value_counts().to_string())

    job_data['CPUTimePerCore'] = job_data[wrap_cpu] / job_data[core_count]
    job_data['CPUDemand'] = job_data[wrap_cpu] * df['HSScorePerCore']

    job_data[cpu_idle_time] = job_data[wrap_wc] * job_data[core_count] - job_data[wrap_cpu]

    jobs_invalid_idle_time = job_data[job_data[cpu_idle_time] < 0]
    logging.debug("Number of jobs with invalid CPU idle time: {} (relative {})".format(jobs_invalid_idle_time.shape[0],
                                                                                       jobs_invalid_idle_time.shape[0] /
                                                                                       job_data.shape[0]))

    max_cpu_time = job_data[wrap_wc] * job_data[core_count]

    # Set invalid values of CPU idle time to NaN
    job_data.loc[(job_data[cpu_idle_time] < 0) | (job_data[cpu_idle_time] > max_cpu_time), cpu_idle_time] = np.nan

    job_data[cpu_idle_time_ratio] = job_data[cpu_idle_time] / max_cpu_time.mask(max_cpu_time <= 0)

    job_data[job_cpu_efficiency] = job_data[wrap_cpu] / max_cpu_time.mask(max_cpu_time <= 0)

    job_data.loc[(job_data[job_cpu_efficiency] < 0) | (job_data[job_cpu_efficiency] > 1), job_cpu_efficiency] = np.nan
    logging.debug("Min job CPU efficiency: {}, max: {}".format(job_data[job_cpu_efficiency].min(),
                                                               job_data[job_cpu_efficiency].max()))

    job_data[Metric.IO_TIME.value] = job_data[Metric.WRITE_TIME.value] + job_data[Metric.READ_TIME.value]

    # Add CPU Time per event
    job_data[Metric.CPU_TIME_PER_EVENT.value] = job_data[Metric.CPU_TIME.value] / job_data[Metric.EVENT_COUNT.value]
    job_data[Metric.CPU_DEMAND_PER_EVENT.value] = job_data['CPUDemand'] / job_data[Metric.EVENT_COUNT.value]

    # Replace infinite values with null values
    job_data[Metric.CPU_TIME_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)
    job_data[Metric.CPU_DEMAND_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)

    # Add CPU Idle time per event
    job_data[Metric.CPU_IDLE_TIME_PER_EVENT.value] = job_data[Metric.CPU_IDLE_TIME.value] / job_data[Metric.EVENT_COUNT.value]
    job_data[Metric.CPU_IDLE_TIME_PER_EVENT.value].replace([np.inf, -np.inf], np.nan, inplace=True)

    histogram.log_value_counts(job_data, Metric.CPU_TIME.value)
    histogram.log_value_counts(job_data, Metric.EVENT_COUNT.value)

    histogram.log_value_counts(job_data, Metric.CPU_TIME_PER_EVENT.value)
    histogram.log_value_counts(job_data, Metric.CPU_DEMAND_PER_EVENT.value)

    return job_data


# Todo This may belong somewhere else!
def add_missing_node_info(df, nodes):
    average_computing_rate = nodes['HSScorePerCore'].mean()
    average_computing_rate_per_jobslot = nodes['HSScorePerJobslot'].mean()

    df_filled = df.copy()
    df_filled['HSScorePerCore'] = df['HSScorePerCore'].fillna(average_computing_rate)
    df_filled['HSScorePerJobslot'] = df['HSScorePerJobslot'].fillna(average_computing_rate_per_jobslot)

    return df_filled


def compute_average_cpu_efficiency(df, start=None, end=None):
    if start is not None:
        df = df[df['Timestamp'] >= start]

    if end is not None:
        df = df[df['Timestamp'] >= end]

    return df['CPUEfficiency'].mean()
