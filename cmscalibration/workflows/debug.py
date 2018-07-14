import logging

import numpy as np
import pandas as pd
from analysis import cpuefficiencyanalysis
from analysis import demandextraction
from analysis import jobmonitoring
from analysis import nodeanalysis
from exporters import demandexport
from exporters import nodetypes
from importers.gridkadata import CPUEfficienciesImporter, GridKaNodeDataImporter, CoreUsageImporter
from importers.jobmonitoring import JobMonitoringImporter
from merge import job_node
from validation import cpuefficiency


def run_workflow():
    jm_importer = JobMonitoringImporter()

    start_date = pd.to_datetime('2018-03-01')
    end_date = pd.to_datetime('2018-05-01')

    jobs = jm_importer.importDataFromFile('./data/output_jobmonitoring_2018-03to04.txt')

    node_importer = GridKaNodeDataImporter()
    nodes = node_importer.importDataFromFile('./data/gridka-benchmarks-2017.csv')

    nodeanalysis.addPerformanceData(nodes)

    matched_jobs = job_node.match_jobs_to_node(jobs, nodes)

    job_data = jobmonitoring.add_performance_data(matched_jobs)

    cpu_efficiency = cpuefficiency.cpu_efficiency(job_data)
    logging.info("Total CPU time / Walltime efficiency: {}".format(cpu_efficiency))
    cpu_efficiency_scaled = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_data)
    logging.info("Total CPU time / Walltime efficiency scaled by jobslot count and virtual cores: {}".format(
        cpu_efficiency_scaled))

    cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots_physical(job_data)
    logging.info("Total CPU time / Walltime efficiency scaled by jobslot count and physical cores: {}".format(
        cpu_efficiency_scaled_physical))

    core_importer = CoreUsageImporter()
    core_usage_cms = core_importer.importDataFromFile('./data/core_usage_data_cms.csv')
    total_cores = core_importer.importDataFromFile('./data/total_available_cores.csv')

    core_usage_cms = core_usage_cms.rename(columns={'Value': 'CMSCores'})
    total_cores = total_cores.rename(columns={'Value': 'TotalCores'})

    logging.debug(core_usage_cms.columns)

    core_df = core_usage_cms[['Timestamp', 'CMSCores']].merge(total_cores[['Timestamp', 'TotalCores']], on='Timestamp')
    core_df['CMSShare'] = core_df['CMSCores'] / core_df['TotalCores']
    core_df[core_df['CMSShare'] > 1] = np.nan

    logging.info("Total mean share for CMS: {}".format(core_df['CMSShare'].mean()))
    mean_core_share_in_timeframe = core_df[(core_df['Timestamp'] < end_date) & (core_df['Timestamp'])].mean()
    logging.info(
        "Mean share for CMS in time frame from {} to {}: {}".format(start_date, end_date, mean_core_share_in_timeframe))

    cms_avg_cores = mean_core_share_in_timeframe['CMSCores']

    logging.info("Mean number of slots for CMS: {}".format(cms_avg_cores))

    node_types = nodeanalysis.extractNodeTypes(nodes)
    scaled_nodes = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    out_directory = './out/params'

    nodetypes.exportToJsonFile(scaled_nodes, '/'.join([out_directory, 'nodes.json']))

    demands = demandextraction.extract_demands(job_data)
    shares = {job_demands['typeName']: job_demands['relativeFrequency'] for job_demands in demands}

    logging.debug(str(shares))

    demandexport.export_to_json_file(demands, '/'.join([out_directory, 'jobs.json']))

    cpu_eff_importer = CPUEfficienciesImporter()
    cpu_eff_df = cpu_eff_importer.importDataFromFile('./data/gridka_cpu_over_walltime.csv')

    cpu_efficiency_data = cpuefficiencyanalysis.compute_average_cpu_efficiency(cpu_eff_df, start=start_date,
                                                                               end=end_date)
    # cpu_efficiency_data = cpuefficiencyanalysis.compute_average_cpu_efficiency(cpu_eff_df)
    logging.debug(
        "CPU Efficiency total from {} to {} (from GridKa perspective, with Pilots): {}".format(start_date, end_date,
                                                                                               cpu_efficiency_data))

    # Remove types that are very infrequent in the data
    # job_data_filtered = jobmonitoring.filter_df_by_type(job_data, 0.0001)

    # job_partitions = jobtypesplit.split_by_type(job_data_filtered, 'Type', True)
    # demand_list = [demandextraction.extract_demands(partition) for partition in job_partitions]
    #
    # for demands in demand_list:
    #     logging.debug("Demands:\n{}".format(demands))

    # demandextraction.extract_demands(job_data)
