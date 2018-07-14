""" Histogram utilities. """

import numpy as np
import pandas as pd


def calculate_histogram_mean(counts, bins):
    """
    Calculate the mean of all values in a histogram, assuming that in each bin,
    the values can be assumed to be uniformly distributed (and hence the bin mean be
    assumed as their mean).
    """
    widths = bins[1:] - bins[:-1]
    frequencies = counts / sum(counts)

    middles = bins[:-1] + (widths / 2)
    mean = middles.dot(frequencies)

    return mean


def bin_by_quantile(x, bin_count=100):
    """Create a quantile-distributed histogram with the specified number of bins from a Pandas series of values."""

    quantiles = x.quantile(np.linspace(0.0, 1.0, num=bin_count + 1))
    bin_edges = [0.0] + quantiles.tolist()

    hist, bins = pd.cut(x, bin_edges, right=False, include_lowest=True, duplicates='drop', retbins=True)
    counts = hist.value_counts(sort=False).values

    return counts, bins


def bin_equal_width_overflow(x, bin_count=100, cutoff_quantile=0.95):
    """Create a histogram with equal-width bins, the specified number of bins from a Pandas series of values.
    This function optionally cuts off outlier values above the provided quantile and handles them by aggregating
    them into a single overflow bin that preserves their arithmetic mean.
    """
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

    return counts, bins
