import logging

import pandas as pd

from .csv import CSVImporter


class JobMonitoringImporter(CSVImporter):

    def __init__(self):
        # TODO Explicitly specify DTypes!
        self.jm_dtypes = {'JobId': int,
                          'FileName': str}

        # TODO Make these parameters
        self.dropped_columns = ['FileName', 'ProtocolUsed']
        self.header = 'JobId,FileName,IsParentFile,ProtocolUsed,SuccessFlag,FileType,LumiRanges,StrippedFiles,BlockId,StrippedBlocks,BlockName,InputCollection,Application,ApplicationVersion,Type,GenericType,NewGenericType,NewType,SubmissionTool,InputSE,TargetCE,SiteName,SchedulerName,JobMonitorId,TaskJobId,SchedulerJobIdV2,TaskId,TaskMonitorId,NEventsPerJob,NTaskSteps,JobExecExitCode,JobExecExitTimeStamp,StartedRunningTimeStamp,FinishedTimeStamp,WrapWC,WrapCPU,ExeCPU,NCores,NEvProc,NEvReq,WNHostName,JobType,UserId,GridName'

    def importDataFromFile(self, path):
        logging.info("Reading jobmonitoring file from {}".format(path))

        self.checkHeader(path, self.header)

        df_raw = pd.read_csv(path, sep=',', dtype=self.jm_dtypes)

        logging.debug("Jobmonitoring dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw jobmonitoring file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop([col for col in self.dropped_columns if col in df_raw.columns], axis='columns')
        df = df.drop_duplicates()

        logging.info("Jobmonitoring file with dropped columns with shape: {}".format(df.shape))
        # logging.debug("Number of distinct JobIDs: {}".format(df.JobId.unique().shape))

        # logging.debug("Number of FileType entries: {}".format(df.FileType.unique()))

        self.regularizeHostNames(".gridka.de", df)
        self.regularize_job_type(df)

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
