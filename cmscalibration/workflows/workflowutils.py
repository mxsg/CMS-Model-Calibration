import os

from exporters.datasetexport import CalibrationParameterExporter
from utils import config


def export_job_counts(job_counts, subdir, name):
    directory = os.path.join(config.outputDirectory, subdir)
    os.makedirs(directory, exist_ok=True)

    path = os.path.join(directory, name)

    job_counts = job_counts[job_counts['count'] > 0]

    job_counts.to_csv(path)


def export_parameters(subdir, node_params, demand_params):
    parameter_path = os.path.join(config.outputDirectory, subdir)

    exporter = CalibrationParameterExporter(parameter_path)
    exporter.export(node_params, 'nodes.json', demand_params, 'jobs.json')
