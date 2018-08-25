import logging

import pandas as pd

from data.dataset import Metric, Dataset
from interfaces.fileimport import MultiFileDataImporter
from utils.calibrationerrors import MissingColumnError


class SummarizedWMAImporter(MultiFileDataImporter):
    """Instances of this class can be used to import WMArchive data from a summarized input format."""

    provided_metrics = {
        'startTime': Metric.START_TIME,
        'stopTime': Metric.STOP_TIME,
        'ts': Metric.TIMESTAMP,

        'task': Metric.WORKFLOW,
        'jobtype': Metric.JOB_CATEGORY,
        'task_name': Metric.TASK_NAME,

        'TotalJobCPU': Metric.CPU_TIME,
        'TotalJobTime': Metric.WALL_TIME,
        'NumberOfThreads': Metric.USED_THREADS,

        'NumberOfStreams': Metric.EVENT_STREAM_COUNT,

        # 'TotalEvents': Metric.EVENT_COUNT,
        'inputEvents': Metric.INPUT_EVENT_COUNT,
        'outputEvents': Metric.OUTPUT_EVENT_COUNT,

        # Todo Maybe rename this?
        'TotalInitTime': Metric.INIT_TIME,
        'EventThroughput': Metric.EVENT_THROUGHPUT,

        # 'readTotalSecs': Metric.READ_TIME,
        'writeTotalSecs': Metric.WRITE_TIME,
        'readTotalSecs': Metric.READ_TIME,
        'ioTotalSecs': Metric.IO_TIME,

        'readTotalMB': Metric.TOTAL_READ_DATA,
        'writeTotalMB': Metric.TOTAL_WRITTEN_DATA,

        'readMBSec': Metric.AVERAGE_READ_SPEED,
        'writeMBSec': Metric.AVERAGE_WRITE_SPEED,

        'exitCode': Metric.EXIT_CODE,
        'wn_name': Metric.HOST_NAME
    }

    def __init__(self, with_files=True):
        """Create a new importer.

        :param with_files: If true, also import the file list associated with each row.
        """
        self.id_column = 'wmaid'
        self.with_files = with_files

        self.date_filter_metric = Metric.STOP_TIME
        self.file_column = 'LFNArray'

        self.timestamp_columns = ['stopTime', 'startTime', 'ts']

        # Keep track of which columns are required for this to importers correctly
        self.required_columns = set(self.provided_metrics.keys())
        self.required_columns.add(self.id_column)
        if with_files:
            self.required_columns.add(self.file_column)

    def import_file(self, path, start_date, end_date):
        return self.import_file_list([path], start_date, end_date)

    def import_file_list(self, path_list, start_date, end_date):
        logging.debug("Importing WMArchive data from paths: {}.".format(path_list))

        wmdf_list = [pd.read_json(path, lines=True) for path in path_list]

        logging.debug("Reading complete.")

        df_raw = pd.concat(wmdf_list)

        # Check if all required columns exist
        # Todo Check again whether required columns exist, but allow additional columns not present in the original data set!
        # missing_columns = set(self.required_columns) - set(df_raw.columns)
        #
        # if len(missing_columns) > 0:
        #     logging.error("Jobmonitoring file has missing columns: {}!".format(missing_columns))
        #     raise MissingColumnError(missing_columns)

        id_duplicates = df_raw[self.id_column].duplicated().sum()
        if id_duplicates > 0:
            logging.warning("WMArchive dataset contains {} duplicated IDs. Dropping them.".format(id_duplicates))
            df_raw = df_raw.drop_duplicates(self.id_column)

        additional_tables = {}
        if self.with_files:
            logging.debug("Extracting files from data.")
            additional_tables['files'] = self._get_file_table(df_raw)
            logging.debug("Files extracted.")

        jobs = df_raw.drop(columns=self.file_column, errors='ignore').set_index(self.id_column)
        jobs = self._convert_columns(jobs)

        # Todo Drop unused columns again!
        wmdf = self._subset_with_spec_columns(jobs)

        # Create dataset
        job_dataset = Dataset(wmdf, name="WMArchive Jobs", start=start_date, end=end_date, extra_dfs=additional_tables)

        self.filter_with_date_range(job_dataset, start_date, end_date)
        return job_dataset

    def filter_with_date_range(self, dataset, start_date, end_date):
        """Filter the dataset to only contain entries between the start and end dates."""

        # TODO Optimize by also filtering the files table.
        # This is currently retained and hence also includes entries that do not correspond
        # to any entry in the jobs table any more.

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

        task_sep = '/'
        df['task_name'] = df['task'].str.split(task_sep).apply(lambda x: x[-1] if len(x) >= 2 else None)

        # Todo Maybe do not overwrite previous values?
        df['task'] = df['task'].str.split('/').apply(lambda x: x[1] if len(x) >= 2 else x[0])

        # Convert mixed case to lowercase
        df['jobtype'] = df['jobtype'].str.lower()

        # Handle storage
        df['readTotalSecs'] = df['readTotalMB'] / df['readMBSec']
        df['writeMBSec'] = df['writeTotalMB'] / df['writeTotalSecs']

        df['ioTotalSecs'] = df['writeTotalSecs'] + df['readTotalSecs']

        # Fill null exit codes with 0
        # df['NumberOfThreads'] = df['NumberOfThreads'].fillna(0)
        df['exitCode'] = df['exitCode'].fillna(0)

        df['eventsFromPerf'] = df['TotalJobTime'] * df['EventThroughput']

        return df

    def _subset_with_spec_columns(self, df):
        # Todo Keep unused columns?
        df = df.drop(columns=[col for col in df.columns if col not in self.provided_metrics.keys()])

        rename_spec = {old_name: metric.value for old_name, metric in self.provided_metrics.items()}
        df = df.rename(columns=rename_spec)
        return df

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
        self._convert_columns(wmdf)
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
