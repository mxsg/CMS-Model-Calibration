from abc import ABCMeta, abstractmethod


class FileDataImporter(metaclass=ABCMeta):
    """Interface to import data contained in a single file or list of files."""

    @abstractmethod
    def import_file(self, path, start_date, end_date):
        return NotImplemented


class MultiFileDataImporter(FileDataImporter, metaclass=ABCMeta):
    """Inteface to import data spread across a list of multiple files."""

    @abstractmethod
    def import_file_list(self, path_list, start_date, end_date):
        return NotImplemented
