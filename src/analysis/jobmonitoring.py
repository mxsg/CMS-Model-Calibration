import logging

import pandas as pd
import numpy as np


def add_performance_data(df):
    required_columns = []
    wrap_cpu = 'WrapCPU'
    wrap_wc = 'WrapWC'
    job_type = 'Type'
    core_count = 'NCores'

    # estimated_io_time = 'estimatedIOTime'
    # estimated_io_time_total = 'estimatedIOTimeBusyTimeTotal'

    cpu_idle_time = 'CPUIdleTime'
    job_cpu_efficiency = 'OverallJobCPUEfficiency'

    job_data = df.copy()

    # TODO Do this properly!
    logging.debug("WrapCPU summary: positive {}, zero {}, negative {}".format(job_data[job_data[wrap_cpu] > 0].shape[0],
                                                                              job_data[job_data[wrap_cpu] == 0].shape[
                                                                                  0],
                                                                              job_data[job_data[wrap_cpu] < 0].shape[
                                                                                  0]))

    logging.debug("WrapWC summary: positive {}, zero {}, negative {}".format(job_data[job_data[wrap_wc] > 0].shape[0],
                                                                             job_data[job_data[wrap_wc] == 0].shape[0],
                                                                             job_data[job_data[wrap_wc] < 0].shape[0]))

    logging.debug("NCores summary:\n" + job_data['NCores'].value_counts().to_string())
    logging.debug("Types summary:\n" + job_data[job_type].value_counts().to_string())

    job_data[job_data[wrap_cpu] < 0] = pd.NaT
    logging.debug("Number of NA values: {}".format(job_data[wrap_cpu].isnull().sum()))

    job_data['CPUTimePerCore'] = job_data['WrapCPU'] / job_data['NCores']
    job_data['CPUDemand'] = job_data['WrapCPU'] * df['HSScorePerCore']

    job_data[cpu_idle_time] = job_data[wrap_wc] * job_data[core_count] - job_data[wrap_cpu]

    jobs_invalid_idle_time = job_data[job_data[cpu_idle_time] < 0]
    logging.debug("Number of jobs with invalid CPU idle time: {} (relative {})".format(jobs_invalid_idle_time.shape[0],
                                                                                       jobs_invalid_idle_time.shape[0] /
                                                                                       job_data.shape[0]))

    max_cpu_time = job_data[wrap_wc] * job_data[core_count]

    # job_data[job_cpu_efficiency] = (job_data[wrap_cpu]).where(job_data[wrap_wc] > 0.0)
    job_data[job_cpu_efficiency] = job_data[wrap_cpu] / max_cpu_time.mask(max_cpu_time <= 0)

    # TODO Is this valid?
    # job_data[job_cpu_efficiency].clip(lower=0.0, upper=1.0, inplace=true)
    # Clamp CPU efficiency by
    job_data.loc[(job_data[job_cpu_efficiency] < 0) | (job_data[job_cpu_efficiency] > 1), job_cpu_efficiency] = np.nan
    logging.debug("Min job CPU efficiency: {}, max: {}".format(job_data[job_cpu_efficiency].min(), job_data[job_cpu_efficiency].max()))

    return job_data