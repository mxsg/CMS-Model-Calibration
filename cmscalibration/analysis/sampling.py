import logging


def split_samples(df, frac=0.5, random_state=None):
    """Split the data frame into two parts where the first is about the supplied fraction
    of the total size of the data frame.
    """

    logging.debug("Sampling from {} job reports ({} share).".format(df.shape[0], frac))
    train = df.sample(frac=frac, random_state=random_state)

    test = df.drop(train.index)

    logging.info(
        "Sampled {} train and {} test reports from {} total job reports ({} share).".format(train.shape[0],
                                                                                            test.shape[0],
                                                                                            df.shape[0], frac))

    return train, test
