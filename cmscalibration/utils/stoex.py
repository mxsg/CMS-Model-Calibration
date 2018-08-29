""" Utilities to convert distributions to Palladio stochastic expressions. """
import math


def hist_to_doublepdf(counts, bins):
    """ Convert a histogram to a double PDF Stochastic Expression. """
    if len(counts) + 1 != len(bins):
        raise ValueError("Mismatching bin and value lengths for histogram.")

    # Normalize counts to unity
    relative_counts = counts / sum(counts)
    right_edges = bins[1:]

    keyword = "DoublePDF"

    components = ['({};{})'.format(edge, count) for (edge, count) in zip(right_edges, relative_counts) if not math.isnan(edge)]
    return '{}[{}]'.format(keyword, ''.join(components))


def to_intpmf(values, counts, simplify=True):
    """ Convert an integer probability mass function to a stochastic expression. """

    if len(counts) != len(values):
        raise ValueError("Mismatching number of values and counts.")
    if len(values) == 0:
        raise ValueError("Cannot construct distribution from empty lists.")

    # If there is only one value, simply return it as a string
    if simplify and len(values) == 1:
        return str(int(values[0]))

    # Normalize counts to unity
    relative_counts = counts / sum(counts)

    keyword = "IntPMF"

    components = ['({};{})'.format(int(value), count) for (value, count) in zip(values, relative_counts)]
    return '{}[{}]'.format(keyword, ''.join(components))
