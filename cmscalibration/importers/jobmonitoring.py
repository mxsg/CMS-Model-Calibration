import logging

import pandas as pd

from .csv import CSVImporter


class JobMonitoringImporter(CSVImporter):

    def __init__(self, timezone_correction=None):
        # TODO Explicitly specify DTypes!
        self.jm_dtypes = {'JobId': int,
                          'FileName': str}

        # TODO Make these parameters
        # self.dropped_columns = ['FileName', 'ProtocolUsed']
        self.dropped_columns = []
        self.key_columns = ['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp']
        self.header = 'JobId,FileName,IsParentFile,ProtocolUsed,SuccessFlag,FileType,LumiRanges,StrippedFiles,BlockId,StrippedBlocks,BlockName,InputCollection,Application,ApplicationVersion,Type,GenericType,NewGenericType,NewType,SubmissionTool,InputSE,TargetCE,SiteName,SchedulerName,JobMonitorId,TaskJobId,SchedulerJobIdV2,TaskId,TaskMonitorId,NEventsPerJob,NTaskSteps,JobExecExitCode,JobExecExitTimeStamp,StartedRunningTimeStamp,FinishedTimeStamp,WrapWC,WrapCPU,ExeCPU,NCores,NEvProc,NEvReq,WNHostName,JobType,UserId,GridName'
        self.kept_columns = ['JobId', 'FileName', 'Type', 'GenericType', 'SubmissionTool', 'InputSE',
                             'TaskJobId', 'TaskId', 'TaskMonitorId', 'JobExecExitCode',
                             'JobExecExitTimeStamp', 'StartedRunningTimeStamp', 'FinishedTimeStamp',
                             'WrapWC', 'WrapCPU', 'NCores', 'NEvProc',
                             'WNHostName', 'JobType']
        self.timezone_correction = timezone_correction

    def importDataFromFile(self, path):
        logging.info("Reading jobmonitoring file from {}".format(path))

        self.checkHeader(path, self.header)

        df_raw = pd.read_csv(path, sep=',', dtype=self.jm_dtypes)

        logging.debug("Jobmonitoring dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw jobmonitoring file read with shape: {}".format(df_raw.shape))

        # df = df_raw.drop([col for col in self.dropped_columns if col in df_raw.columns], axis='columns')
        df = df_raw[self.kept_columns].copy()
        df = df.drop_duplicates()

        logging.info("Jobmonitoring file with dropped columns with shape: {}".format(df.shape))
        # logging.debug("Number of distinct JobIDs: {}".format(df.JobId.unique().shape))

        # logging.debug("Number of FileType entries: {}".format(df.FileType.unique()))

        self.regularizeHostNames(".gridka.de", df)
        self.regularize_job_type(df)
        self.preprocess_jm(df)

        # Convert to time stamps

        time_stamp_columns = ['StartedRunningTimeStamp', 'FinishedTimeStamp', 'JobExecExitTimeStamp']
        time_stamps_in_data = [col for col in time_stamp_columns if col in df.columns]

        for col in time_stamps_in_data:
            # Filter out invalid time stamps and then find the first valid date in the data set
            earliest_valid_epoch = df.loc[df[col] > 0, col].min()
            earliest_datetime = pd.to_datetime(earliest_valid_epoch, unit='ms', origin='unix')

            df[col] = pd.to_datetime(df[col], unit='ms', origin='unix')

            # Reset invalid datetimes
            df.loc[df[col] < earliest_datetime, col] = pd.NaT

            # Count number of entries with invalid time stamps
            logging.debug("Invalid with conversion to datetime {} count: {}".format(col, df[col].isnull().sum()))

            logging.debug("Col {}: first date {}, last date {}".format(col, df[col].min(), df[col].max()))

        # TODO This may not be valid in all cases or for all data sets!
        # Make this configurable for the importer!
        if self.timezone_correction is not None:
            self.correct_timestamps(df, time_stamp_columns, self.timezone_correction)

        if 'JobExecTimeStamp' in df.columns:
            logging.debug("Number of mismatching time stamps (Exit vs. Finished): {}"
                          .format(df[df['FinishedTimeStamp'] != df['JobExecExitTimeStamp']].shape[0]))

        return df

    def regularizeHostNames(self, site_suffix, df):
        logging.debug("Regularizing Host Names")

        df['WNHostName.raw'] = df['WNHostName']

        df.WNHostName.replace('{}$'.format(site_suffix), '', regex=True, inplace=True)

        logging.debug("Host Name Count before {}, after {}"
                      .format(df['WNHostName.raw'].unique().shape[0], df.WNHostName.unique().shape[0]))

    def regularize_job_type(self, df):
        df['JobType'] = df['JobType'].str.lower()

    def preprocess_jm(self, jmdf):
        # Drop the first slash from the file name, if present
        jmdf['FileName'] = jmdf['FileName'].replace('^//', '/', regex=True)
        jmdf['TaskMonitorId.raw'] = jmdf['TaskMonitorId']
        jmdf['TaskMonitorId'] = jmdf['TaskMonitorId'].replace('^wmagent_', '', regex=True)

    def correct_timestamps(self, jmdf, ts_columns, tz_string='UTC'):

        target_timezone = 'UTC'

        if target_timezone == tz_string:
            return

        for col in ts_columns:
            # Save uncorrected time stamp in another column
            jmdf[col + '.raw'] = jmdf[col]
            jmdf[col] = jmdf[col].dt.tz_localize('UTC').dt.tz_convert(tz_string).dt.tz_localize(None)
