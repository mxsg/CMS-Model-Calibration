import hashlib


def hash_values(values):
    h = hashlib.md5()

    for value in values:
        h.update(str(value).encode('utf-8'))

    return h.hexdigest()


def hash_row(row, columns):
    return hash_values([row[column] for column in columns])


def hash_builtin(row, columns):
    return hash(tuple(row[column] for column in columns))


def hash_columns(df, columns):
    # return df.apply(lambda x: hash_values([x[column] for column in columns]), axis='columns')
    return df.apply(lambda x: hash_row(x, columns), axis=1)
