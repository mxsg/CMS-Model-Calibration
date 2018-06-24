def hist_to_doublepdf(counts, bins):
    """
    Converts a histogram to a double PDF Stochastic Expression.
    """
    if len(counts) + 1 != len(bins):
        raise ValueError("Mismatching bin and value lengths for histogram.")

    # Normalize counts to unity
    relative_counts = counts / sum(counts)
    right_edges = bins[1:]

    keyword = "DoublePDF"

    # TODO Was: "({:.8f};{:.8f})"
    components = ['({};{})'.format(edge, count) for (edge, count) in zip(right_edges, relative_counts)]
    return '{}[{}]'.format(keyword, ''.join(components))


def to_intpmf(values, counts, simplify=True):
    if len(counts) != len(values):
        raise ValueError("Mismatching number of values and counts.")
    if len(values) == 0:
        raise ValueError("Cannot construct distribution from empty lists.")

    # If there is only one value, simply return it as a string
    if simplify and len(values) == 1:
        return str(values[0])

    # Normalize counts to unity
    relative_counts = counts / sum(counts)

    keyword = "IntPMF"

    components = ['({};{})'.format(value, count) for (value, count) in zip(values, relative_counts)]
    return '{}[{}]'.format(keyword, ''.join(components))
