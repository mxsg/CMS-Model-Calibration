import json
import os

import pandas as pd

from interfaces.fileimport import MultiFileDataImporter


class DatasetDescription:
    """
    Manages a set of files containing data that is be split up into files containing different time periods.
    Such a dataset is described in a JSON description file which is loaded upon construction of the dataset description
    instance.
    """

    def __init__(self, description_path):
        """Construct a new dataset description by loading it from a JSON file."""

        with open(description_path, 'r') as file:
            description = json.load(file)

        self._name = description.get('name', 'dataset')

        file_list = description.get('files')
        if not file_list:
            raise ValueError("Datasets cannot be empty!")

        self._files = pd.DataFrame({'file': desc.get('file'), 'start': pd.to_datetime(desc.get('start')),
                                    'end': pd.to_datetime(desc.get('end'))}
                                   for desc in file_list)

    def files_for_period(self, start, end):
        """
        Return all files that contain data between the supplied start and end dates.

        :param start: The start date.
        :param end: The end date of the query.
        :return: A list of paths to the files that are relevant for the supplied time period.
        """
        df = self._files

        # Find all files with intersecting periods
        matches = df[(df['start'] < start) & (df['end'] > start) |  # Starts before
                     (df['start'] < end) & (df['end'] > start) |  # Ends after
                     (df['start'] >= start) & (df['end'] <= end)]  # Is inside

        return matches['file'].tolist()


class DatasetImporter:
    """Instances of this class can be used to import datasets consisting of multiple files with data."""

    def __init__(self, file_data_importer: MultiFileDataImporter):
        self._importer = file_data_importer

    def import_dataset(self, dataset_description_path, start_date, end_date):
        """Import data from a dataset, only importing files that intersect with the supplied time frame."""

        base_path = os.path.dirname(dataset_description_path)

        description = DatasetDescription(dataset_description_path)
        file_names = description.files_for_period(start_date, end_date)
        file_paths = [os.path.join(base_path, name) for name in file_names]

        dataset = self._importer.import_file_list(file_paths, start_date, end_date)

        return dataset
