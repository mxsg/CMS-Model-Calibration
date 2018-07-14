from abc import ABCMeta, abstractmethod


class FileDataImporter(metaclass=ABCMeta):
    """Interface to import data contained in a single file or list of files."""

    @abstractmethod
    def import_file(self, path, start_date, end_date):
        return NotImplemented

    @abstractmethod
    def import_file_list(self, path_list, start_date, end_date):
        return NotImplemented
