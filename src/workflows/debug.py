import logging

from analysis import demandextraction
from analysis import jobmonitoring
from analysis import jobtypesplit
from analysis import nodeanalysis
from importers.jobmonitoring import JobMonitoringImporter
from importers.nodedata import GridKaNodeDataImporter
from merge import job_node
from exporters import demandexport


def run_workflow():
    jm_importer = JobMonitoringImporter()
    jobs = jm_importer.importDataFromFile('../data/job_data.csv')

    node_importer = GridKaNodeDataImporter()
    nodes = node_importer.importDataFromFile('../data/gridka-benchmarks-2017.csv')

    nodeanalysis.addPerformanceData(nodes)

    matched_jobs = job_node.match_jobs_to_node(jobs, nodes)

    job_data = jobmonitoring.add_performance_data(matched_jobs)

    demands = demandextraction.extract_demands(job_data)

    demandexport.export_to_json_file(demands, '../out/jobs.json')

    # Remove types that are very infrequent in the data
    # job_data_filtered = jobmonitoring.filter_df_by_type(job_data, 0.0001)

    # job_partitions = jobtypesplit.split_by_type(job_data_filtered, 'Type', True)
    # demand_list = [demandextraction.extract_demands(partition) for partition in job_partitions]
    #
    # for demands in demand_list:
    #     logging.debug("Demands:\n{}".format(demands))

    # demandextraction.extract_demands(job_data)


