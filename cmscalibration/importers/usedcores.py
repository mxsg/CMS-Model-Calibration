import logging

import pandas as pd

from .csv import CSVImporter


class CoreUsageImporter(CSVImporter):
    def __init__(self):
        self.dropped_columns = []

    def importDataFromFile(self, path):
        logging.info("Reading Core Usage data from file {}".format(path))

        df_raw = pd.read_csv(path, sep=';')

        logging.debug("Core usage dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw core usage file read with shape: {}".format(df_raw.shape))

        df = df_raw
        # df = df_raw.drop(self.dropped_columns, axis='columns')
        # df = df.drop_duplicates()

        # df.rename(columns={'Time': 'Timestamp', 'Value': 'CPUEfficiency'}, inplace=True)

        df['Timestamp'] = pd.to_datetime(df['Time'])
        # df['Core'] = df['Value']/100

        logging.debug("Core usage dtypes:")
        logging.debug(df.dtypes)

        logging.debug("Earliest timestamp in core counts file: {}, latest: {}".format(df['Timestamp'].min(), df['Timestamp'].max()))

        logging.info("Core usage with dropped columns with shape: {}".format(df.shape))

        return df
