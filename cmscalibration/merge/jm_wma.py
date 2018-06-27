import logging

import pandas as pd


def match_jobs(jmdf, wmdf):
    logging.debug("Matching data frames, jmdf with {} entries, wma {}.".format(jmdf.shape[0], wmdf.shape[0]))
    jmdf = jmdf.copy()
    wmdf = wmdf.copy()

    preprocess_jm(jmdf)

    key_cols = ['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp']
    file_col = 'FileName'
    unique_id = add_unique_id(jmdf, key_cols)
    jmdf = jmdf.set_index(unique_id)

    jm_filenames = jmdf[[unique_id, file_col]]

    wma_key = 'wmaid'
    wmdf = wmdf.drop_duplicates(wma_key)

    # wma_filenames =


def preprocess_jm(jmdf):
    # Drop the first slash from the file name, if present
    jmdf['FileName'] = jmdf['FileName'].replace('^//', '/', regex=True)


def add_unique_id(jmdf, keys, id_name='uniqueId'):
    """
    Adds a unique identifier to the data frame. Note that this operation modifies the passed data frame. This either
    return a column from the supplied keys or creates a new column called id_name.

    :param jmdf: The data frame the unique identifier should be added to. This data frame is modified.
    :param keys: The columns to be used as keys for the data frame. The  unique identifier will be the same if and only
    if their combination is identical.
    :return: The name of the column chosen as identifier, either a value from keys or a new column
    """

    # This operation is potentially slow, so test whether one of the supplied keys is already adequate
    length = jmdf.shape[0]

    for key in keys:
        if jmdf.duplicated(key).sum() == 0:
            return key

    # No key has been found, create one from a combination of the keys.
    def make_identifier(df):
        str_id = df.apply(lambda x: '_'.join(map(str, x)), axis=1)
        return pd.factorize(str_id)[0]

    jmdf[id_name] = make_identifier(jmdf[keys])
    return id_name
