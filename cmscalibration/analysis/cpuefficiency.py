import pandas as pd

from data.dataset import Metric

wrap_cpu = Metric.CPU_TIME.value
wrap_wc = Metric.WALL_TIME.value

core_count = Metric.USED_CORES.value
cpu_time_per_core = Metric.CPU_TIME_PER_CORE


def cpu_efficiency(df, include_zero_cpu=False):
    """Compute the CPU efficiency from a data frame containing job monitoring information."""

    df_filtered = filter_cpu_efficiency(df, include_zero=include_zero_cpu)

    df_filtered['max_cpu_time'] = df_filtered[wrap_wc] * df_filtered[core_count]

    # Do not count NaN values here
    total_walltime = df_filtered['max_cpu_time'].sum()

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime


def filter_cpu_efficiency(df, cols=None, include_zero=False):

    if not cols:
        cols = [Metric.WALL_TIME.value, Metric.CPU_TIME.value]

    df_filtered = df.copy()
    for col in cols:
        if include_zero:
            mask = df_filtered[col] >= 0
        else:
            mask = df_filtered[col] > 0

        df_filtered = df_filtered[mask]

    return df_filtered


def calculate_efficiencies(jobs: pd.DataFrame, freq='D'):
    df = jobs[[Metric.STOP_TIME.value, Metric.WALL_TIME.value, Metric.CPU_TIME.value, Metric.USED_CORES.value]].copy()

    df = filter_cpu_efficiency(df, include_zero=False)

    df['MaxCPUTime'] = df[Metric.WALL_TIME.value] * df[Metric.USED_CORES.value]

    df['day'] = df[Metric.STOP_TIME.value].dt.round(freq)

    timeseries = df.groupby('day').apply(lambda x: x[Metric.CPU_TIME.value].sum() / x['MaxCPUTime'].sum())

    overall_efficiency = cpu_efficiency(jobs, include_zero_cpu=False)

    return timeseries, overall_efficiency


def cpu_efficiency_scaled_by_jobslots(df, include_zero_cpu=False, physical=False):
    """Compute the CPU efficiency from a data frame containing job monitoring information,
    but scale the result with the number of jobslots available in the node, either with physical or logical cores.
    """
    df_filtered = filter_cpu_efficiency(df, include_zero=include_zero_cpu)

    if physical:
        core_col = 'cores'
    else:
        core_col = 'coresLogical'

    total_walltime = \
        (df_filtered[wrap_wc] * df_filtered[core_count] * df_filtered[core_col] / df_filtered['jobslots']).sum()

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime
