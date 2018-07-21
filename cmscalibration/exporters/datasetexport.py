import json
import logging
import os

from interfaces.fileexport import JSONExporter


class CalibrationParameterExporter:
    """A dataset exporter can be used to export a full calibration parameter set to a single location."""

    def __init__(self, base_path=''):
        self.base_path = base_path

        self.node_exporter = NodeTypeExporter()
        self.job_exporter = DemandExporter()

    def set_path(self, path):
        self.base_path = path

    def export(self, node_types, node_file_name, job_demands, job_file_name):
        """Export a set of calibration parameters to the specified location."""

        logging.info("Exporting calibration parameters to path {}.".format(self.base_path))

        # Make sure the path exists
        os.makedirs(self.base_path, exist_ok=True)

        path = os.path.join(self.base_path, node_file_name)
        self.node_exporter.export_to_json_file(node_types, path)

        path = os.path.join(self.base_path, job_file_name)
        self.job_exporter.export_to_json_file(job_demands, path)

        logging.info("Finished exporting calibration parameters.")


class NodeTypeExporter(JSONExporter):

    def export_to_json_file(self, node_types, path):
        logging.info("Exporting node types to file: {}".format(path))

        cols = ['name', 'cores', 'jobslots', 'computingRate', 'nodeCount']

        df = node_types[cols]
        df = df.sort_values(by=['nodeCount'], ascending=False)

        node_dict = df.to_dict(orient='records')

        with open(path, 'w') as outfile:
            json.dump(node_dict, outfile, indent=4, sort_keys=True)

        logging.info("Finished exporting node types.")


class DemandExporter(JSONExporter):
    """Instances of this class can be used to export job type resource demands."""

    def export_to_json_file(self, job_type_demands, path):
        logging.info("Exporting node types to file: {}".format(path))
        required_metrics = ['typeName',
                            'cpuDemandStoEx',
                            'ioTimeStoEx',
                            'ioTimeRatioStoEx',
                            'requiredJobslotsStoEx',
                            'relativeFrequency']

        for item in job_type_demands:
            # Check if all required metrics are present in dictionaries
            if not all(metric in item for metric in required_metrics):
                raise ValueError(
                    "Cannot export demands, not all columns present. Required: {}, Encountered: {}".format(
                        required_metrics,
                        job_type_demands.keys()))

        with open(path, 'w') as outfile:
            json.dump(job_type_demands, outfile, indent=4, sort_keys=True)

        logging.info("Finished exporting job types.")
