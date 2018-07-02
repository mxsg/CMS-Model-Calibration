import ast
import logging

import pandas as pd


class WMArchiveSummarizedImporter:

    def from_file(self, path):
        return self.import_from_json(path)

    def from_file_list(self, path_list):
        df_list = [self.from_file(path) for path in path_list]
        return pd.concat(df_list).drop_duplicates('wmaid')

    def import_from_json(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        wmdf = pd.read_json(path, lines=True)
        self.convert_columns(wmdf)
        return wmdf


    def import_from_file(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        data = []

        with open(path) as f:
            for line in f:
                # Todo Catch exceptions here!
                try:
                    record = ast.literal_eval(line)
                except ValueError as e:
                    print("Invalid entry found, continuing. Entry: \n{}".format(line))
                    print(str(e))
                    continue

                data.append(record)

        wmdf = pd.DataFrame(data).drop_duplicates('wmaid')
        self.convert_columns(wmdf)

        return wmdf

    def convert_columns(self, df):
        df['startTime'] = pd.to_datetime(df['startTime'], unit='s')
        df['stopTime'] = pd.to_datetime(df['stopTime'], unit='s')
        df['cmsrunStartTime'] = pd.to_datetime(df['cmsrunStartTime'], unit='s')
        df['wmats'] = pd.to_datetime(df['wmats'], unit='s')
        df['ts'] = pd.to_datetime(df['ts'], unit='s')
        df['TaskMonitorId'] = df['task'].str.split('/').apply(lambda x: x[1])