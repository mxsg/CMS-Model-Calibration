from abc import ABCMeta, abstractmethod


class JSONExporter(metaclass=ABCMeta):
    """Interface to export data to JSON files."""

    @abstractmethod
    def export_to_json_file(self, data, path):
        return NotImplemented
