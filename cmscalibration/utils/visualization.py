import numpy as np
import numpy as np

import matplotlib.pyplot as plt


def draw_binned_data(counts, bins):
    fig, axes = plt.subplots()

    centroids = (bins[1:] + bins[:-1]) / 2

    axes.hist(centroids, density=True, bins=bins, weights=counts, histtype='step')

    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)

    return fig, axes


def draw_integer_distribution(values, counts):
    fig, axes = plt.subplots()

    min_value = np.min(values)
    max_bin = np.max(values)

    bins = np.arange(min_value, max_bin + 1)
    vals = np.zeros(max_bin - min_value + 1)

    for k, v in zip(values, counts):
        vals[k - min_value] = v

    axes.bar(bins, vals)

    return fig, axes
