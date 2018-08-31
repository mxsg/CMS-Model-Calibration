import math

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

    axes.set_xlabel('')
    axes.legend()
    axes.set_ylim([0, 1])

    fig.tight_layout()

    return fig, axes


class MultiPlotFigure:
    """Objects of this class can be used to iteratively draw multiple subplots into the same figure."""

    def __init__(self, nplots, ncols=2):
        self.nplots = nplots
        self.ncols = ncols
        self.nrows = math.ceil(nplots / ncols)

        self.maxplots = self.nrows * self.ncols

        self.i_next_subplot = 0
        self.fig, self.axes_list = plt.subplots(ncols=self.ncols, nrows=self.nrows)

    @property
    def current_axis(self):
        return self.axes_list[self.i_next_subplot // self.ncols, self.i_next_subplot % self.ncols]

    def finish_subplot(self):
        if self.i_next_subplot >= self.nplots:
            raise ValueError(
                "Cannot step to next plot with number {}, figure is already full!".format(self.i_next_subplot + 1))
        else:
            self.i_next_subplot += 1

    def add_to_report(self, report, identifier, width=10, height=10):

        self.fig.set_size_inches(width, height)

        # Remove all plots that were not used
        for i in range(self.i_next_subplot, self.maxplots):
            self.fig.delaxes(self.axes_list[i // self.ncols, i % self.ncols])

        # Add plot to report
        report.add_figure(self.fig, self.axes_list, identifier)
