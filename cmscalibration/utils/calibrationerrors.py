""" Framework specific error classes. """


class MissingColumnError(Exception):
    """ Indicates that a column is missing for this operation. """

    def __init__(self, columns=None, file=None):
        self.columns = columns
        self.file = file
