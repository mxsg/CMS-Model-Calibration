class MissingColumnError(Exception):

    def __init__(self, columns=None, file=None):
        self.columns = columns
        self.file = file
