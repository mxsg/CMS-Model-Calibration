import os

from exporters.datasetexport import CalibrationParameterExporter
from utils import config, report as rp



def export_job_counts(job_counts, subdir, name):
    directory = os.path.join(config.output_directory, subdir)
    os.makedirs(directory, exist_ok=True)

    path = os.path.join(directory, name)

    job_counts = job_counts[job_counts['count'] > 0]

    job_counts.to_csv(path)


def export_parameters(subdir, node_params, demand_params, report: rp.ReportBuilder = None):
    parameter_path = os.path.join(config.output_directory, subdir)

    exporter = CalibrationParameterExporter(parameter_path)
    exporter.export(node_params, 'nodes.json', demand_params, 'jobs.json')

    # Todo Use another solution for writing out reports to different locations?
    if report is not None:
        base_path = report.get_base_path()
        report.set_base_path(parameter_path)

        report.write()

        # Reset base path
        report.set_base_path(base_path)