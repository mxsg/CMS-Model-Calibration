from abc import ABCMeta, abstractmethod


class FileDataImporter(metaclass=ABCMeta):

    @abstractmethod
    def import_file(self, path, start_date, end_date):
        return NotImplemented

    @abstractmethod
    def import_file_list(self, path_list, start_date, end_date):
        return NotImplemented
