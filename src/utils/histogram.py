def calculate_histogram_mean(counts, bins):
    widths = bins[1:] - bins[:-1]
    frequencies = counts / sum(counts)

    middles = bins[:-1] + (widths / 2)
    mean = middles.dot(frequencies)

    return mean
