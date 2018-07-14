""" Histogram utilities. """


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
