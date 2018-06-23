import logging

import pandas as pd
import numpy as np

from .csv import CSVImporter

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
