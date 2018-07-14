""" Contains usability functions to allow to create unique identifiers in a data frame. """

import hashlib


def hash_values(values):
    """ Create an MD5 has from a list of values representable as string.

    :param values: The values to hash.
    :return: An MD5 hash computed from the string representation of the
    supplied values used as a seed.
    """
    h = hashlib.md5()

    for value in values:
        h.update(str(value).encode('utf-8'))

    return h.hexdigest()


def hash_row(row, columns):
    """ Create an MD5 hash from selected columns in a data frame row. """
    return hash_values([row[column] for column in columns])


def hash_builtin(row, columns):
    """ Create a Python builtin hash from the selected columns in a data frame row. """
    return hash(tuple(row[column] for column in columns))


def hash_columns(df, columns):
    """ Return a series of hashes created from the provided columns in the data frame. """
    return df.apply(lambda x: hash_row(x, columns), axis=1)
