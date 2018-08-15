import pandas as pd

from interfaces.fileimport import FileDataImporter


class JobCountImporter(FileDataImporter):

    def import_file(self, path, start_date, end_date):
        df = pd.read_csv(path, header=None)

        df.columns = ['date', 'num1', 'count', 'type']
        df = df.drop(columns='num1')

        df['date'] = pd.to_datetime(df['date'])

        df = df[(df['date'] >= start_date) & (df['date'] < end_date)]

        return df