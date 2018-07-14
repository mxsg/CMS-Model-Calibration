import logging

import numpy as np


def add_jobmonitoring_performance_data(df):
    wrap_cpu = 'CPUTime'
    wrap_wc = 'WallTime'
    job_type = 'JobType'
    core_count = 'UsedCores'

    cpu_idle_time = 'CPUIdleTime'
    job_cpu_efficiency = 'OverallJobCPUEfficiency'
    cpu_idle_time_ratio = 'CPUIdleTimeRatio'

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

    return job_data


def compute_average_cpu_efficiency(df, start=None, end=None):
    if start is not None:
        df = df[df['Timestamp'] >= start]

    if end is not None:
        df = df[df['Timestamp'] >= end]

    return df['CPUEfficiency'].mean()