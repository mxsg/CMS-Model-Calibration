import logging


def split_by_column_value(df, colname, copy=False):
    """ Split a data frame into partitions based on the value of a column.

    :param df: The data frame to split
    :param colname: The name of the column to split by.
    :param copy: If true, return copies of the data frames.
    :return: A dictionary containing the column value as keys and the data frames belonging to
    the value as values.
    """
    logging.debug("Splitting data frame by column.")
    values = df[colname].unique()

    if copy:
        partitions = {value: df[df[colname] == value].copy() for value in values}
    else:
        partitions = {value: df[df[colname] == value] for value in values}

    return partitions
