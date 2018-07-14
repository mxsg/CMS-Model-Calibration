from data.dataset import Metric

wrap_cpu = Metric.CPU_TIME.value
wrap_wc = Metric.WALL_TIME.value

core_count = Metric.USED_CORES.value
cpu_time_per_core = Metric.CPU_TIME_PER_CORE


def cpu_efficiency(df, include_zero_cpu=False):
    """Compute the CPU efficiency from a data frame containing job monitoring information."""

    df_filtered = df[df[wrap_wc] > 0]

    if include_zero_cpu:
        df_filtered = df_filtered[df_filtered[wrap_cpu] >= 0]
    else:
        df_filtered = df_filtered[df_filtered[wrap_cpu] > 0]

    total_walltime = df_filtered[wrap_wc].dot(df_filtered[core_count])

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime


def cpu_efficiency_scaled_by_jobslots(df, include_zero_cpu=False, physical=False):
    """Compute the CPU efficiency from a data frame containing job monitoring information,
    but scale the result with the number of jobslots available in the node, either with physical or logical cores.
    """
    df_filtered = df[df[wrap_wc] > 0]

    if include_zero_cpu:
        df_filtered = df_filtered[df_filtered[wrap_cpu] >= 0]
    else:
        df_filtered = df_filtered[df_filtered[wrap_cpu] > 0]

    if physical:
        core_col = 'cores'
    else:
        core_col = 'coresLogical'

    total_walltime = \
        (df_filtered[wrap_wc] * df_filtered[core_count] * df_filtered[core_col] / df_filtered['jobslots']).sum()

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime
