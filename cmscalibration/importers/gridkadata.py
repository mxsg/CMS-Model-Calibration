import logging

import pandas as pd

from importers.csv import CSVImporter
from .csv import CSVImporter


class CPUEfficienciesImporter(CSVImporter):
    def __init__(self):
        self.dropped_columns = []

    def importDataFromFile(self, path):
        logging.info("Reading CPU efficiency data from file {}".format(path))

        df_raw = pd.read_csv(path, sep=';')

        logging.debug("CPU Efficiency dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw CPU efficiency file read with shape: {}".format(df_raw.shape))

        df = df_raw
        # df = df_raw.drop(self.dropped_columns, axis='columns')
        # df = df.drop_duplicates()

        # df.rename(columns={'Time': 'Timestamp', 'Value': 'CPUEfficiency'}, inplace=True)

        df['Timestamp'] = pd.to_datetime(df['Time'])
        df['CPUEfficiency'] = df['Value']/100

        logging.debug("CPU Efficiency dtypes:")
        logging.debug(df.dtypes)

        logging.debug("Earliest timestamp in cpu efficiencies file: {}, latest: {}".format(df['Timestamp'].min(), df['Timestamp'].max()))

        logging.info("CPU Efficiency with dropped columns with shape: {}".format(df.shape))

        return df


class GridKaNodeDataImporter(CSVImporter):

    def __init__(self):
        self.header = 'hostname,jobslots,hs06,db12-at-boot,db12cpp-at-boot,db12numpy-at-boot,cores,cpu model,interconnect'

        self.dropped_columns = []

    def importDataFromFile(self, path):

        logging.info("Reading GridKa node data from file {}".format(path))

        self.checkHeader(path, self.header)

        df_raw = pd.read_csv(path, sep=',')

        logging.debug("Nodes dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw GridKa node type file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop(self.dropped_columns, axis='columns')
        df = df.drop_duplicates()

        logging.info("GridKa node type file with dropped columns with shape: {}".format(df.shape))

        return df


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