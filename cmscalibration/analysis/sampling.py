def split_samples(df, frac=0.5, random_state=None):
    """
    Splits the data frame into two parts where the first is about the supplied fraction
    of the total size of the data frame.
    """

    if random_state is None:
        train = df.sample(frac=frac)
    else:
        train = df.sample(frac, random_state=random_state)

    test = df.drop(train.index)

    return train, test
