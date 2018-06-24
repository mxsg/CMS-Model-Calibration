import logging

import numpy as np
import pandas as pd

from utils import histogram


def extract_demands(df):
    demands = {}

    df_filtered = filter_invalid_data(df)

    # This filters jobs by only analyzing those of a type of the frequency at least shown here
    min_rel_type_frequency = 0.0001
    df_filtered = filter_df_by_type(df_filtered, min_rel_type_frequency)

    counts, bins = extract_cpu_demand_distribution(df_filtered)

    return demands


def extract_cpu_demand_distribution(df, demand_col='CPUDemand', percentile_cutoff=1.00, bin_count=100):
    logging.debug("Extracting CPU demand distribution.")

    logging.debug(
        "CPU Demands in Data: min {}, max {}, mean {}".format(df[demand_col].min(), df[demand_col].max(),
                                                              df[demand_col].mean()))

    cutoff_demand = df[demand_col].quantile(percentile_cutoff)
    df_cutoff = df[df[demand_col] <= cutoff_demand]

    logging.debug(
        "CPU demands after cutoff: min {}, max {}, mean {}".format(df_cutoff[demand_col].min(),
                                                                   df_cutoff[demand_col].max(),
                                                                   df_cutoff[demand_col].mean()))

    quantiles = df_cutoff[demand_col].quantile(np.linspace(0.0, 1.0, num=bin_count + 1))
    bin_edges = [0.0] + quantiles.tolist()

    hist, bins = pd.cut(df_cutoff[demand_col], bin_edges, right=False, include_lowest=True, duplicates='drop',
                        retbins=True)
    # rel_hist = hist / hist.sum()

    return hist, bins


def bin_by_quantile(x, bin_count=100):
    quantiles = x.quantile(np.linspace(0.0, 1.0, num=bin_count + 1))
    bin_edges = [0.0] + quantiles.tolist()

    hist, bins = pd.cut(x, bin_edges, right=False, include_lowest=True, duplicates='drop', retbins=True)
    counts = hist.value_counts(sort=False).values

    logging.debug(
        "Binning by quantile, distribution mean: {}".format(histogram.calculate_histogram_mean(bins, counts)))

    return counts, bins


def bin_equal_width_overflow(x, bin_count=100, cutoff_quantile=0.95):
    if cutoff_quantile < 0.0 or cutoff_quantile >= 1.0:
        raise ValueError("Quantile must be between 0.0 and <1.0.")

    cutoff = x.quantile(cutoff_quantile)
    x_cutoff = x[x <= cutoff]
    x_overflow = x[x > cutoff]

    # Compute width of overflow bin by aggregating with mean of the overflowed values
    overflow_mean = x_overflow.mean()
    overflow_width = 2 * (overflow_mean - cutoff)
    overflow_right = cutoff + overflow_width

    bin_edges = np.linspace(0.0, cutoff, num=bin_count + 1)

    # Add the last value to the histogram
    bin_edges = np.append(bin_edges, np.array(x.max()))

    hist, bins = pd.cut(x, bin_edges, right=False, include_lowest=True, retbins=True)

    counts = hist.value_counts(sort=False).values
    bins[-1] = overflow_right

    logging.debug(
        "Binning with equal width bins, distribution mean: {}, cutoff: {}".format(
            histogram.calculate_histogram_mean(bins, counts), cutoff))

    return counts, bins


def filter_invalid_data(df):
    logging.debug("Number of entries before: {}".format(df.shape[0]))

    # Remove data with zero walltime but nonzero CPU time
    df_subset = df.drop(df[(df['WrapCPU'] > 0) & (df['WrapWC'] <= 0)].index)

    # Remove data with zero CPU time but non-zero Walltime
    df_subset = df_subset.drop(df_subset[(df_subset['WrapCPU'] <= 0) & (df_subset['WrapWC'] > 0)].index)

    logging.debug("Number of entries after: {}".format(df_subset.shape[0]))

    return df_subset


def filter_df_by_type(df, min_rel_freq):
    filtered_frequencies = filter_job_frequencies(df, min_rel_freq)
    filtered_df = df.loc[df['Type'].isin(filtered_frequencies.index)].copy()

    return filtered_df, filtered_frequencies


def filter_job_frequencies(df, min_rel_freq):
    rel_frequencies = df['Type'].value_counts() / len(df)

    filtered_frequencies = rel_frequencies[rel_frequencies < min_rel_freq]

    # Normalize after dropping
    filtered_frequencies = filtered_frequencies / sum(filtered_frequencies)
    return filtered_frequencies
