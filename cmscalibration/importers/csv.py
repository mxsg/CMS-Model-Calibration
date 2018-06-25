import logging

class CSVImporter:

    def __init__(self):
        pass

    def checkHeader(self, path, header):
        with open(path, 'r') as file:
            header_line = file.readline().rstrip()

            if header != header_line:
                logging.warn("Header mismatch in file {}:".format(path))
                logging.warn("Expected: {}".format(header))
                logging.warn("Encountered: {}".format(header_line))
            else:
                logging.debug("Matched expected header format")
