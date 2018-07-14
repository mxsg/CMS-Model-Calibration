import logging

import pandas as pd

from utils.calibrationerrors import MissingColumnError
from data.dataset import Metric, Dataset
from interfaces.fileimport import MultiFileDataImporter
from utils import unique_identifier
from utils.report import ReportingEntity


class JMImporter(ReportingEntity, MultiFileDataImporter):
    """Instances of this class allow the import of JobMonitoring data."""

    # TODO Split this up into required and optional columns to be more flexible

    jm_dtypes = {
        'JobId': int,
        'FileName': str,
        'Type': str,
        'GenericType': str,
        'SubmissionTool': str,
        'InputSE': str,
        'TaskJobId': int,
        'TaskId': int,
        'TaskMonitorId': str,
        'JobExecExitCode': float,  # Contains null values
        'JobExecExitTimeStamp': int,
        'StartedRunningTimeStamp': int,
        'FinishedTimeStamp': int,
        'WrapWC': float,
        'WrapCPU': float,
        'NCores': int,
        'NEvProc': int,
        'WNHostName': str,
        'JobType': str,
        'UniqueID': str
    }

    defined_metrics = {
        'Type': Metric.JOB_TYPE,
        'JobType': Metric.JOB_CATEGORY,
        'SubmissionTool': Metric.SUBMISSION_TOOL,
        'TaskMonitorId': Metric.WORKFLOW,

        'WNHostName': Metric.HOST_NAME,

        'JobExecExitCode': Metric.EXIT_CODE,

        'StartedRunningTimeStamp': Metric.START_TIME,
        'JobExecExitTimeStamp': Metric.STOP_TIME,
        'FinishedTimeStamp': Metric.FINISHED_TIME,

        'WrapWC': Metric.WALL_TIME,
        'WrapCPU': Metric.CPU_TIME,
        'NCores': Metric.USED_CORES,
        'NEvProc': Metric.EVENT_COUNT,
    }

    def __init__(self, timezone_correction=None, hostname_suffix='', with_files=True, report_builder=None):
        super().__init__(report_builder=report_builder)
        self.timezone_correction = timezone_correction
        self.hostname_suffix = hostname_suffix
        self.with_files = with_files

        self.key_columns = ['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp']
        self.time_stamp_columns = ['StartedRunningTimeStamp', 'FinishedTimeStamp', 'JobExecExitTimeStamp']

        self.id_column = 'UniqueID'
        self.file_column = 'FileName'

        self.date_filter_metric = Metric.FINISHED_TIME

        self.required_columns = set(self.defined_metrics.keys()) | set(self.key_columns)
        if self.with_files:
            self.required_columns.add(self.file_column)

    def import_file(self, path, start_date=None, end_date=None):
        return self.import_file_list([path], start_date, end_date)

    def import_file_list(self, path_list, start_date=None, end_date=None):
        logging.debug("Reading Jobmonitoring files from paths {}.".format(path_list))

        df_list = [pd.read_csv(path, sep=',', dtype=self.jm_dtypes) for path in path_list]
        df = pd.concat(df_list)

        self.report.append("# Jobmonitoring Import")

        logging.debug("Finished reading Jobmonitoring file with {} entries. Now converting data.".format(df.shape[0]))

        job_dataset = self._convert_raw_data_to_dataset(df, start=start_date, end=end_date)
        logging.debug("Finished converting Jobmonitoring dataset with {} jobs.".format(job_dataset.df.shape[0]))

        logging.debug("Filtering to jobs between dates {} and {}.".format(start_date, end_date))
        jobs_all_dates = job_dataset.df.shape[0]
        self.filter_with_date_range(job_dataset, start_date, end_date)
        logging.debug("Jobs before filtering {}, after {}.".format(jobs_all_dates, job_dataset.df.shape[0]))

        return job_dataset

    def filter_with_date_range(self, dataset, start_date, end_date):
        """Filter the data in the supplied dataset to only contain data within the provided time frame."""

        jobs = dataset.df

        # Filter entries based on jobs' stop date
        if start_date:
            jobs = jobs[jobs[dataset.col(self.date_filter_metric)] >= start_date]
        if end_date:
            jobs = jobs[jobs[dataset.col(self.date_filter_metric)] <= end_date]

        dataset.df = jobs

    def _convert_raw_data_to_dataset(self, df, start=None, end=None):
        # Check if all required columns exist
        missing_columns = set(self.required_columns) - set(df.columns)

        if len(missing_columns) > 0:
            logging.error("Jobmonitoring file has missing columns: {}!".format(missing_columns))
            raise MissingColumnError(missing_columns)

        # Drop all unimportant columns inplace
        df = self._filter_columns(df)

        # Drop duplicates from the dataset
        df.drop_duplicates(inplace=True)

        if self.id_column in df.columns:
            logging.debug("Found ID column in Jobmonitoring data.")
        else:
            logging.info("Could not find ID column in Jobmonitoring data.")
            logging.info("Generating unique ID from columns {}. This may take a while.".format(self.key_columns))
            df[self.id_column] = unique_identifier.hash_columns(df, self.key_columns)

        # Set up table with files for later setting it in the dataset
        additional_tables = {}
        if self.with_files:
            additional_tables['files'] = self._get_file_table(df)

        # Drop files from the dataset
        jobs = df.drop(columns=self.file_column).drop_duplicates()

        id_duplicate_sum = jobs.duplicated(self.id_column).sum()
        if id_duplicate_sum > 0:
            logging.warning("Found {} duplicates in IDs of jobs, dropping them.".format(id_duplicate_sum))

        logging.debug("Found {} jobs in file.".format(jobs.shape[0]))

        jobs = self._convert_columns(jobs)

        # Subset and rename based on metrics
        jobs = jobs.set_index(self.id_column)

        # Only keep columns defined in metric dictionary
        jobs = self._subset_with_spec_columns(jobs)

        # Convert into Dataset and return
        job_dataset = Dataset(jobs, name="Jobmonitoring Jobs", start=start, end=end, extra_dfs=additional_tables)

        return job_dataset

    def _convert_columns(self, jmdf: pd.DataFrame):

        host_column = 'WNHostName'
        host_names_before = jmdf[host_column].nunique()
        jmdf[host_column] = self._standardize_host_names(jmdf[host_column], self.hostname_suffix)
        host_names_after = jmdf[host_column].nunique()

        logging.debug("Standardized host names, before {}, after {}".format(host_names_before, host_names_after))

        jmdf['JobType'] = jmdf['JobType'].str.lower()

        # Remove prefix from task monitor ID
        jmdf['TaskMonitorId'] = jmdf['TaskMonitorId'].replace('^wmagent_', '', regex=True)

        # Convert to time stamps
        for col in self.time_stamp_columns:
            jmdf[col] = self._convert_timestamps(jmdf[col])

            if self.timezone_correction:
                jmdf[col] = self._correct_timestamps(jmdf[col], tz_string=self.timezone_correction)

        return jmdf

    def _filter_columns(self, df):

        # Drop all columns not contained in the dtypes specification
        df = df.drop([col for col in df.columns if col not in self.jm_dtypes.keys()], axis=1, errors='ignore')
        return df

    def _subset_with_spec_columns(self, df):
        df = df.drop(columns=[col for col in df.columns if col not in self.defined_metrics.keys()])

        rename_spec = {old_name: metric.value for old_name, metric in self.defined_metrics.items()}
        df = df.rename(columns=rename_spec)
        return df

    def _get_file_table(self, df):

        # Drop all but important columns
        df = df[[self.id_column, self.file_column]]

        files = df.drop_duplicates().reset_index(drop=True)
        return files

    def _get_file_df(self, jmdf):
        files = jmdf[[self.id_column, self.file_column]].drop_duplicates().reset_index(drop=True)

        # Remove double slashes in the beginning from files
        files[self.file_column] = jmdf[self.file_column].replace('^//', '/', regex=True)

        return files

    @staticmethod
    def _correct_timestamps(series, tz_string='UTC'):
        target_timezone = 'UTC'

        if target_timezone == tz_string:
            return

        # Correct time stamp by converting to UTC and then dropping the time zone information
        series = series.dt.tz_localize('UTC').dt.tz_convert(tz_string).dt.tz_localize(None)
        return series

    @staticmethod
    def _convert_timestamps(series):
        # Filter out invalid time stamps and then find the first valid date in the data set
        earliest_valid_epoch = series[series > 0].min()
        earliest_datetime = pd.to_datetime(earliest_valid_epoch, unit='ms', origin='unix')

        timestamps = pd.to_datetime(series, unit='ms', origin='unix')

        # Reset invalid datetimes, i.e. all before the earliest non-null epoch
        timestamps.loc[timestamps < earliest_datetime] = pd.NaT

        return timestamps

    @staticmethod
    def _standardize_host_names(column, suffix):
        column = column.str.replace('{}$'.format(suffix), '', regex=True)
        return column
