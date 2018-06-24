from analysis import nodeanalysis
from analysis import jobmonitoring

from exporters import nodetypes
from merge import job_node
from importers.jobmonitoring import JobMonitoringImporter
from importers.nodedata import GridKaNodeDataImporter

import logging


def run_workflow():
    jm_importer = JobMonitoringImporter()
    jobs = jm_importer.importDataFromFile('../data/output_jobmonitoring_2018-03.txt')

    node_importer = GridKaNodeDataImporter()
    nodes = node_importer.importDataFromFile('../data/gridka-benchmarks-2017.csv')

    nodeanalysis.addPerformanceData(nodes)

    matched_jobs = job_node.match_jobs_to_node(jobs, nodes)
    job_data = jobmonitoring.add_performance_data(matched_jobs)




