import logging

import pandas as pd
import numpy as np

from .csv import CSVImporter

class JobMonitoringImporter(CSVImporter):

    def __init__(self):
        # TODO Explicitly specify DTypes!
        self.jm_dtypes = {'JobId': int,
                          'FileName': str}

        # TODO Make these parameters
        self.dropped_columns = ['FileName', 'ProtocolUsed']
        self.header =  'JobId,FileName,IsParentFile,ProtocolUsed,SuccessFlag,FileType,LumiRanges,StrippedFiles,BlockId,StrippedBlocks,BlockName,InputCollection,Application,ApplicationVersion,Type,GenericType,NewGenericType,NewType,SubmissionTool,InputSE,TargetCE,SiteName,SchedulerName,JobMonitorId,TaskJobId,SchedulerJobIdV2,TaskId,TaskMonitorId,NEventsPerJob,NTaskSteps,JobExecExitCode,JobExecExitTimeStamp,StartedRunningTimeStamp,FinishedTimeStamp,WrapWC,WrapCPU,ExeCPU,NCores,NEvProc,NEvReq,WNHostName,JobType,UserId,GridName'

    def importDataFromFile(self, path):

        logging.info("Reading jobmonitoring file from {}".format(path))

        self.checkHeader(path, self.header)

        df_raw = pd.read_csv(path, sep=',', dtype=self.jm_dtypes)

        logging.debug("Jobmonitoring dtypes:")
        logging.debug(df_raw.dtypes)

        logging.info("Raw jobmonitoring file read with shape: {}".format(df_raw.shape))

        df = df_raw.drop(self.dropped_columns, axis='columns')
        df = df.drop_duplicates()

        logging.info("Jobmonitoring file with dropped columns with shape: {}".format(df.shape))
        logging.debug("Number of distinct JobIDs: {}".format(df.JobId.unique().shape))

        # logging.debug("Number of FileType entries: {}".format(df.FileType.unique()))

        self.regularizeHostNames(".gridka.de", df)

        return df


    def regularizeHostNames(self, site_suffix, df):

        logging.debug("Regularizing Host Names")

        df['WNHostName.raw'] = df['WNHostName']

        df.WNHostName.replace('{}$'.format(site_suffix), '', regex=True, inplace=True)

        logging.debug("Host Name Count before {}, after {}"
            .format(df['WNHostName.raw'].unique().shape[0], df.WNHostName.unique().shape[0]))
