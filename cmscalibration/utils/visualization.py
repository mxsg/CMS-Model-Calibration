import matplotlib.pyplot as plt


def draw_binned_data(counts, bins):
    fig, axes = plt.subplots()

    draw_binned_data_subplot(counts, bins, axes)

    return fig, axes


def draw_binned_data_subplot(counts, bins, axes, name=''):
    centroids = (bins[1:] + bins[:-1]) / 2
    total = sum(counts)

    axes.hist(centroids, density=True, bins=bins, weights=counts, histtype='step')

    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)

    axes.set_title("{} (from {} jobs)".format(name, total))

    return axes


def draw_integer_distribution(values, counts, norm=True, name=''):
    fig, axes = plt.subplots()

    # min_value = np.min(values)
    # max_bin = np.max(values)

    # bins = np.arange(min_value, max_bin + 1)
    # vals = np.zeros(max_bin - min_value + 1)

    # for k, v in zip(values, counts):
    #     vals[k - min_value] = v

    axes = draw_integer_distribution_subplot(values, counts, axes, norm, name)

    return fig, axes


def draw_integer_distribution_subplot(values, counts, axes, norm=True, name=''):
    total = sum(counts)
    shares = [x / total for x in counts]

    # Either draw total number or shares
    bars = shares if norm else counts

    axes.bar(values, bars)

    axes.set_title("{} (from {} jobs)".format(name, total))

    axes.set_xlabel(r"Number of required slots")
    if norm:
        axes.set_ylabel("Share")
        axes.set_ylim(0, 1)
    else:
        axes.set_ylabel("Number of Jobs")

    return axes


def draw_efficiency_timeseries(series_dict, resample_freq=None):
    fig, axes = plt.subplots()

    for name, series in series_dict.items():
        # Resample time series
        if resample_freq is not None:
            series = series.resample(resample_freq).mean()

        label = "{} (average {:.2f}%)".format(name, series.mean() * 100)
        series.plot.line(ax=axes, label=label)

    # axes.set_xlabel('Time')
    axes.set_xlabel('')
    axes.legend()
    axes.set_ylim([0, 1])

    fig.tight_layout()

    return fig, axes
