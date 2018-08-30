import logging

import numpy as np
import pandas as pd

from data.dataset import Metric
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

        if not check_header(path, self.header):
            raise ValueError("Did not match expected header format for node data!")

        df_raw = pd.read_csv(path, sep=',')

        logging.debug("Nodes dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw GridKa node type file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop(self.dropped_columns, axis='columns')
        df = df.drop_duplicates()

        metrics = {
            'hs06': Metric.BENCHMARK_TOTAL.value,
            'jobslots': Metric.JOBSLOT_COUNT.value,
            'hostname': Metric.HOST_NAME.value,
            'cpu model': Metric.CPU_NAME.value,
            'cores': Metric.PHYSICAL_CORE_COUNT.value,
            'interconnect': Metric.INTERCONNECT_TYPE.value
        }

        if not set(metrics.keys()).issubset(set(df.columns)):
            raise ValueError("Missing column in node data!")

        df = df.rename(columns=metrics)

        logging.info("GridKa node type file with shape: {} and columns: {}".format(df.shape, df.columns))

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
                                                                   on='Timestamp', how='outer')
        core_df[share_col] = core_df[partial_col] / core_df[total_col]

        core_df.set_index('Timestamp', inplace=True)

        # Reset invalid entries
        core_df[core_df[share_col] > 1] = np.nan

        return core_df


class ColumnCoreUsageImporter(FileDataImporter):
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

        df.set_index('Timestamp', inplace=True)

        logging.info("Core usage with dropped columns with shape: {}".format(df.shape))

        return df


class CPUEfficiencyReferenceImporter(FileDataImporter):

    def __init__(self, col='cms', output_column='value'):
        super().__init__()

        self.dataset_type = 'CPU Efficiency'
        self.col = col
        self.output_column = output_column

    def import_file(self, path, start_date, end_date):
        logging.info("Reading {} data from file {}".format(self.dataset_type, path))

        df = pd.read_csv(path, sep=';')
        logging.info("Raw {} file read with shape: {}".format(self.dataset_type, df.shape))

        df['Timestamp'] = pd.to_datetime(df['Time'])

        # Subset data to match time span
        df = df[(df['Timestamp'] >= start_date) & (df['Timestamp'] <= end_date)]

        df.set_index('Timestamp', inplace=True)
        df.rename(columns={self.col: self.output_column}, inplace=True)
        df[self.output_column] = df[self.output_column].divide(100)

        logging.info("{} with shape: {}".format(self.dataset_type, df.shape))

        return df


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
