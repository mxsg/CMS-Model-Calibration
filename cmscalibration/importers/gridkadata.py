import logging

import numpy as np
import pandas as pd

from interfaces.fileimport import FileDataImporter


class CPUEfficienciesImporter(FileDataImporter):
    """Imports time series information about CPU efficiencies in the format provided by GridKa."""

    def __init__(self):
        super().__init__()
        self.dropped_columns = []

    def import_file(self, path, start_date, end_date):
        logging.info("Reading CPU efficiency data from file {}".format(path))

        df_raw = pd.read_csv(path, sep=';')

        logging.debug("CPU Efficiency dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw CPU efficiency file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop(self.dropped_columns, axis='columns')
        df = df.drop_duplicates()

        df['Timestamp'] = pd.to_datetime(df['Time'])
        df['CPUEfficiency'] = df['Value'] / 100

        # Subset data to match time span
        df = df[(df['Timestamp'] >= start_date) & (df['Timestamp'] <= end_date)]

        logging.info("CPU Efficiency with dropped columns with shape: {}".format(df.shape))

        return df


class GridKaNodeDataImporter:
    """Imports node information with the format provided by GridKa."""

    def __init__(self):
        self.header = \
            'hostname,jobslots,hs06,db12-at-boot,db12cpp-at-boot,db12numpy-at-boot,cores,cpu model,interconnect'

        self.dropped_columns = []

    def import_file(self, path):
        logging.info("Reading GridKa node data from file {}".format(path))

        check_header(path, self.header)

        df_raw = pd.read_csv(path, sep=',')

        logging.debug("Nodes dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw GridKa node type file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop(self.dropped_columns, axis='columns')
        df = df.drop_duplicates()

        logging.info("GridKa node type file with dropped columns with shape: {}".format(df.shape))

        return df


class CoreUsageImporter(FileDataImporter):
    """Imports time series information about the count of used cores in the format provided by GridKa."""

    def __init__(self):
        super().__init__()
        self.dropped_columns = []

    def import_file(self, path, start_date, end_date):
        logging.info("Reading Core Usage data from file {}".format(path))

        df = pd.read_csv(path, sep=';')
        logging.info("Raw core usage file read with shape: {}".format(df.shape))

        df['Timestamp'] = pd.to_datetime(df['Time'])

        # Subset data to match time span
        df = df[(df['Timestamp'] >= start_date) & (df['Timestamp'] <= end_date)]

        logging.info("Core usage with dropped columns with shape: {}".format(df.shape))

        return df

    def import_core_share(self, path_total, path_share, start_date, end_date,
                          share_col='coreShare', partial_col='corePartial', total_col='coreTotal'):
        """Import core usage information from separate files for total available cores and a share of the cores
        (e.g. for a single user of the total resource usage).
        """

        usage_share = self.import_file(path_share, start_date, end_date)
        usage_total = self.import_file(path_total, start_date, end_date)

        core_usage_cms = usage_share.rename(columns={'Value': partial_col})
        total_cores = usage_total.rename(columns={'Value': total_col})

        core_df = core_usage_cms[['Timestamp', partial_col]].merge(total_cores[['Timestamp', total_col]],
                                                                   on='Timestamp')
        core_df[share_col] = core_df[partial_col] / core_df[total_col]

        # Reset invalid entries
        core_df[core_df[share_col] > 1] = np.nan

        return core_df


def check_header(path, header):
    """Compare the header of a file with the provided header format."""

    with open(path, 'r') as file:
        header_line = file.readline().rstrip()

        if header != header_line:
            logging.warning("Header mismatch in file {}:".format(path))
            logging.warning("Expected: {}".format(header))
            logging.warning("Encountered: {}".format(header_line))
            return False
        else:
            logging.debug("Matched expected header format")
            return True
