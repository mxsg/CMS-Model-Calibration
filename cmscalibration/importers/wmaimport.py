import logging

import pandas as pd

from data.dataset import Metric, Dataset
from interfaces.fileimport import FileDataImporter
from utils.calibrationerrors import MissingColumnError


class SummarizedWMAImporter(FileDataImporter):
    provided_metrics = {
        'startTime': Metric.START_TIME,
        'stopTime': Metric.STOP_TIME,
        'ts': Metric.TIMESTAMP,

        'task': Metric.WORKFLOW,
        'jobtype': Metric.JOB_CATEGORY,

        'TotalJobCPU': Metric.CPU_TIME,
        'TotalJobTime': Metric.WALL_TIME,
        'NumberOfThreads': Metric.USED_CORES,

        # 'TotalEvents': Metric.EVENT_COUNT,
        'inputEvents': Metric.INPUT_EVENT_COUNT,
        'outputEvents': Metric.OUTPUT_EVENT_COUNT,

        'readTotalSecs': Metric.READ_TIME,
        'writeTotalSecs': Metric.WRITE_TIME,
        'readAveragekB': Metric.AVERAGE_READ_SPEED,
        'readTotalMB': Metric.TOTAL_READ_MB,
        'writeTotalMB': Metric.TOTAL_WRITE_MB,

        'exitCode': Metric.EXIT_CODE,
        'wn_name': Metric.HOST_NAME
    }

    def __init__(self, with_files=True):
        self.id_column = 'wmaid'
        self.with_files = with_files

        self.date_filter_metric = Metric.STOP_TIME
        self.file_column = 'LFNArray'

        self.timestamp_columns = ['stopTime', 'startTime', 'ts']

        # Keep track of which columns are required for this to import correctly
        self.required_columns = set(self.provided_metrics.keys())
        self.required_columns.add(self.id_column)
        if with_files:
            self.required_columns.add(self.file_column)

    def import_file(self, path, start_date, end_date):
        return self.import_file_list([path], start_date, end_date)

    def import_file_list(self, path_list, start_date, end_date):
        logging.debug(f"Importing WMArchive data from paths: {path_list}.")

        wmdf_list = [pd.read_json(path, lines=True) for path in path_list]

        logging.debug(f"Reading complete.")

        df_raw = pd.concat(wmdf_list)

        # Check if all required columns exist
        missing_columns = set(self.required_columns) - set(df_raw.columns)

        if len(missing_columns) > 0:
            logging.error(f"Jobmonitoring file has missing columns: {missing_columns}!")
            raise MissingColumnError(missing_columns)

        id_duplicates = df_raw[self.id_column].duplicated().sum()
        if id_duplicates > 0:
            logging.warning(f"WMArchive dataset contains {id_duplicates} duplicated IDs. Dropping them.")
            df_raw = df_raw.drop_duplicates(self.id_column)

        additional_tables = {}
        if self.with_files:
            logging.debug("Extracting files from data.")
            additional_tables['files'] = self._get_file_table(df_raw)
            logging.debug("Files extracted.")

        jobs = df_raw.drop(columns=self.file_column).set_index(self.id_column)
        jobs = self._convert_columns(jobs)

        wmdf = self._subset_with_spec_columns(jobs)

        # Create dataset
        job_dataset = Dataset(wmdf, name="WMArchive Jobs", start=start_date, end=end_date, extra_dfs=additional_tables)

        self.filter_with_date_range(job_dataset, start_date, end_date)
        return job_dataset

    # TODO This does not filter the files table. Optimize by doing this!
    def filter_with_date_range(self, dataset, start_date, end_date):
        jobs = dataset.df

        # Filter entries based on jobs' stop date
        if start_date:
            jobs = jobs[jobs[dataset.col(self.date_filter_metric)] >= start_date]
        if end_date:
            jobs = jobs[jobs[dataset.col(self.date_filter_metric)] <= end_date]

        dataset.df = jobs

    def _convert_columns(self, df):

        for timestamp_col in self.timestamp_columns:
            df[timestamp_col] = self._convert_timestamps(df[timestamp_col])

        df['task'] = df['task'].str.split('/').apply(lambda x: x[1] if len(x) >= 2 else None)

        # Convert into MiB
        df['readAveragekB'] = df['readAveragekB'] / 1024

        # Fill null exit codes with 0
        df['NumberOfThreads'] = df['NumberOfThreads'].fillna(0)

        return df

    def _subset_with_spec_columns(self, df):
        df = df.drop(columns=[col for col in df.columns if col not in self.provided_metrics.keys()])

        rename_spec = {old_name: metric.value for old_name, metric in self.provided_metrics.items()}
        df = df.rename(columns=rename_spec)
        return df

    # TODO Put this into a utils module?
    @staticmethod
    def _convert_timestamps(series):
        # Filter out invalid time stamps and then find the first valid date in the data set
        earliest_valid_epoch = series[series > 0].min()
        earliest_datetime = pd.to_datetime(earliest_valid_epoch, unit='s', origin='unix')

        timestamps = pd.to_datetime(series, unit='s', origin='unix')

        # Reset invalid datetimes, i.e. all before the earliest non-null epoch
        timestamps.loc[timestamps < earliest_datetime] = pd.NaT

        return timestamps

    def _read_json(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        wmdf = pd.read_json(path, lines=True)
        self.convert_columns(wmdf)
        return wmdf

    def _get_file_table(self, wmdf, additional_cols=None):
        if additional_cols is None:
            additional_cols = []

        wmdf = wmdf[[self.id_column, self.file_column] + additional_cols]

        # Files from WMArchive table
        s = wmdf.apply(lambda x: pd.Series(x[self.file_column]), axis=1).stack().reset_index(level=1, drop=True)
        s.name = 'FileName'
        wm_files = wmdf.drop(self.file_column, axis=1).join(s)
        wm_files = wm_files[['FileName', self.id_column] + additional_cols].drop_duplicates().reset_index(drop=True)
        return wm_files
