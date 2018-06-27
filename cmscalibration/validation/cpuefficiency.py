def cpu_efficiency(df, include_zero_cpu=False):
    required_columns = []

    wrap_cpu = 'WrapCPU'
    wrap_wc = 'WrapWC'

    core_count = 'NCores'
    cpu_time_per_core = 'CPUTimePerCore'

    df_filtered = df[df[wrap_wc] > 0]

    if include_zero_cpu:
        df_filtered = df_filtered[df_filtered[wrap_cpu] >= 0]
    else:
        df_filtered = df_filtered[df_filtered[wrap_cpu] > 0]

    total_walltime = df_filtered[wrap_wc].dot(df_filtered[core_count])

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime

def cpu_efficiency_scaled_by_jobslots(df, include_zero_cpu=False):
    required_columns = []

    wrap_cpu = 'WrapCPU'
    wrap_wc = 'WrapWC'

    core_count = 'NCores'
    cpu_time_per_core = 'CPUTimePerCore'

    df_filtered = df[df[wrap_wc] > 0]

    if include_zero_cpu:
        df_filtered = df_filtered[df_filtered[wrap_cpu] >= 0]
    else:
        df_filtered = df_filtered[df_filtered[wrap_cpu] > 0]

    # total_walltime = df_filtered[wrap_wc].dot(df_filtered[core_count])
    total_walltime = (df_filtered[wrap_wc] * df_filtered[core_count] * df_filtered['coresLogical'] / df_filtered['jobslots']).sum()

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime

def cpu_efficiency_scaled_by_jobslots_physical(df, include_zero_cpu=False):
    required_columns = []

    wrap_cpu = 'WrapCPU'
    wrap_wc = 'WrapWC'

    core_count = 'NCores'
    cpu_time_per_core = 'CPUTimePerCore'

    df_filtered = df[df[wrap_wc] > 0]

    if include_zero_cpu:
        df_filtered = df_filtered[df_filtered[wrap_cpu] >= 0]
    else:
        df_filtered = df_filtered[df_filtered[wrap_cpu] > 0]

    # total_walltime = df_filtered[wrap_wc].dot(df_filtered[core_count])
    total_walltime = (df_filtered[wrap_wc] * df_filtered[core_count] * df_filtered['cores'] / df_filtered['jobslots']).sum()

    total_cpu_time = df_filtered[wrap_cpu].sum()

    return total_cpu_time / total_walltime

