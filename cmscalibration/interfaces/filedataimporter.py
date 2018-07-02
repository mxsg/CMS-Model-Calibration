from abc import ABCMeta, abstractmethod


class FileDataImporter(meta=ABCMeta):

    @abstractmethod
    def from_file(self, path):
        return NotImplemented

    @abstractmethod
    def from_file_list(self, path_list):
        return NotImplemented
