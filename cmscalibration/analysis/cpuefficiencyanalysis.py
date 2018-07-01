def compute_average_cpu_efficiency(df, start=None, end=None):
    if start is not None:
        df = df[df['Timestamp'] >= start]

    if end is not None:
        df = df[df['Timestamp'] >= end]

    return df['CPUEfficiency'].mean()
