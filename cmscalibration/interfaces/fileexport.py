from abc import ABCMeta, abstractmethod


class JSONExporter(metaclass=ABCMeta):

    @abstractmethod
    def export_to_json_file(self, data, path):
        return NotImplemented
