import logging


def split_by_column_value(df, colname, copy=False):
    logging.debug("Splitting data frame by column.")
    values = df[colname].unique()

    if copy:
        partitions = {value: df[df[colname] == value].copy() for value in values}
    else:
        partitions = {value: df[df[colname] == value] for value in values}

    return partitions
