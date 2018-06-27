import logging
import os

import numpy as np
import pandas as pd
from analysis import cpuefficiencyanalysis
from analysis import demandextraction
from analysis import jobmonitoring
from analysis import nodeanalysis
from analysis import sampling
from exporters import demandexport
from exporters import nodetypes
from importers.cpuefficiencies import CPUEfficienciesImporter
from importers.jobmonitoring import JobMonitoringImporter
from importers.nodedata import GridKaNodeDataImporter
from importers.usedcores import CoreUsageImporter
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
    scaled_nodes = nodeanalysis.scale_site_to_jobslot_count(node_types, cms_avg_cores)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    cpu_eff_importer = CPUEfficienciesImporter()
    cpu_eff_df = cpu_eff_importer.importDataFromFile('./data/gridka_cpu_over_walltime.csv')

    cpu_efficiency_data = cpuefficiencyanalysis.compute_average_cpu_efficiency(cpu_eff_df, start=start_date,
                                                                               end=end_date)
    # cpu_efficiency_data = cpuefficiencyanalysis.compute_average_cpu_efficiency(cpu_eff_df)
    logging.debug(
        "CPU Efficiency total from {} to {} (from GridKa perspective, with Pilots): {}".format(start_date, end_date,
                                                                                               cpu_efficiency_data))

    out_parent = './out/params'

    ## Create full parameter set

    out_directory = os.path.join(out_parent, "full")
    info_file = 'info.txt'

    logging.debug("===== Job count before dropping: {}".format(job_data.shape[0]))

    job_subset = job_data[(job_data['StartedRunningTimeStamp'] >= start_date) &
                          (job_data['FinishedTimeStamp'] < end_date)].copy()

    job_subset = job_subset.drop_duplicates(['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp'])

    logging.debug("===== Job count after dropping: {}".format(job_subset.shape[0]))

    filename = os.path.join(out_directory, 'nodes.json')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    nodetypes.exportToJsonFile(scaled_nodes, filename)

    demands = demandextraction.extract_demands(job_subset)

    # TODO Refactor this into its own method!
    day_count = (end_date - start_date).days

    summary = job_subset.groupby('Type').size().reset_index()
    summary.columns = ['Type', 'Count']
    summary['countPerDay'] = summary['Count'] / day_count
    summary['relFrequency'] = summary['Count'] / summary['Count'].sum()

    filename = os.path.join(out_directory, 'jobs.json')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    demandexport.export_to_json_file(demands, filename)

    filename = os.path.join(out_directory, info_file)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        file.write("Jobs between {} and {}\n".format(start_date, end_date))
        file.write("\n")

        file.write("Job types (all types):\n")
        file.write(summary.to_string() + "\n")
        file.write("\n")

        file.write("Total jobs: {}\n".format(job_subset.shape[0]))
        file.write("Total days: {}\n".format(day_count))
        file.write("Jobs per day: {}\n".format(summary['countPerDay'].sum()))
        file.write("\n")

        cpu_efficiency = cpuefficiency.cpu_efficiency(job_subset)
        cpu_efficiency_scaled = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_subset)
        cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots_physical(job_subset)

        file.write("## Data from job reports:\n")
        file.write("Total CPU time / Walltime efficiency: {}\n".format(cpu_efficiency))
        file.write("Total CPU time / Walltime efficiency scaled by jobslot count and virtual cores: {}\n".format(
            cpu_efficiency_scaled))
        file.write("Total CPU time / Walltime efficiency scaled by jobslot count and physical cores: {}\n".format(
            cpu_efficiency_scaled_physical))
        file.write("Mean number of pilot slots for CMS: {}\n".format(cms_avg_cores))
        file.write("\n")

        file.write("## Data from other sources (FULL data set):\n")
        file.write(
            "Total CPU Efficiency from {} to {} (from GridKa perspective, with Pilots): {}".format(start_date,
                                                                                                   end_date,
                                                                                                   cpu_efficiency_data))

    sample_shares = [0.1, 0.2, 0.4, 0.5, 0.6, 0.8, 0.9]

    for sample_share in sample_shares:
        out_share_dir = os.path.join(out_parent, "share_{}".format(sample_share))

        samples_subdirs = ['sample1', 'sample2']
        info_file = 'info.txt'

        logging.debug("===== Job count before dropping: {}".format(job_data.shape[0]))

        job_subset = job_data[(job_data['StartedRunningTimeStamp'] >= start_date) &
                              (job_data['FinishedTimeStamp'] < end_date)].copy()

        job_subset = job_subset.drop_duplicates(['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp'])

        logging.debug("===== Job count after dropping: {}".format(job_subset.shape[0]))

        samples = sampling.split_samples(job_subset, frac=sample_share)

        for i, sample in enumerate(samples):
            out_subdirectory = os.path.join(out_share_dir, samples_subdirs[i])
            filename = os.path.join(out_subdirectory, 'nodes.json')
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            nodetypes.exportToJsonFile(scaled_nodes, filename)

            demands = demandextraction.extract_demands(sample)

            # TODO Refactor this into its own method!
            day_count = (end_date - start_date).days

            summary = sample.groupby('Type').size().reset_index()
            summary.columns = ['Type', 'Count']
            summary['countPerDay'] = summary['Count'] / day_count
            summary['relFrequency'] = summary['Count'] / summary['Count'].sum()

            sample_job_count = summary['Count'].sum()

            shares = {job_demands['typeName']: job_demands['relativeFrequency'] for job_demands in demands}

            logging.debug(str(shares))

            filename = os.path.join(out_subdirectory, 'jobs.json')
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            demandexport.export_to_json_file(demands, filename)

            sample_share = sample_job_count / job_subset.shape[0]

            filename = os.path.join(out_subdirectory, info_file)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as file:
                file.write("Jobs between {} and {}\n".format(start_date, end_date))
                file.write("Sampled jobs (sampleID {}), {} of total {} entries (share {})\n".format(samples_subdirs[i],
                                                                                                    sample_job_count,
                                                                                                    job_subset.shape[0],
                                                                                                    sample_job_count /
                                                                                                    job_subset.shape[
                                                                                                        0]))
                file.write("\n")

                file.write("Job types (all types):\n")
                file.write(summary.to_string() + "\n")
                file.write("\n")

                file.write("Total jobs: {}\n".format(sample_job_count))
                file.write("Total days: {}\n".format(day_count))
                file.write("Jobs per day: {}\n".format(summary['countPerDay'].sum()))
                file.write("All data valid for the single sample, sample share {}\n".format(sample_share))
                file.write("\n")

                file.write("## Data scaled up for the total data set based on sample share:\n")
                file.write("Total jobs: {}\n".format(sample_job_count / sample_share))
                file.write("Total days: {}\n".format(day_count))
                file.write("Jobs per day: {}\n".format(summary['countPerDay'].sum() / sample_share))
                file.write("\n")

                cpu_efficiency = cpuefficiency.cpu_efficiency(job_subset)
                cpu_efficiency_scaled = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_subset)
                cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots_physical(job_subset)

                file.write("## Data from job reports (FULL data set):\n")
                file.write("Total CPU time / Walltime efficiency: {}\n".format(cpu_efficiency))
                file.write(
                    "Total CPU time / Walltime efficiency scaled by jobslot count and virtual cores: {}\n".format(
                        cpu_efficiency_scaled))
                file.write(
                    "Total CPU time / Walltime efficiency scaled by jobslot count and physical cores: {}\n".format(
                        cpu_efficiency_scaled_physical))
                file.write("Mean number of pilot slots for CMS: {}\n".format(cms_avg_cores))
                file.write("\n")

                file.write("## Data from job reports (ONLY single sample):\n")
                cpu_efficiency = cpuefficiency.cpu_efficiency(sample)
                cpu_efficiency_scaled = cpuefficiency.cpu_efficiency_scaled_by_jobslots(sample)
                cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots_physical(sample)
                file.write("Total CPU time / Walltime efficiency: {}\n".format(cpu_efficiency))
                file.write(
                    "Total CPU time / Walltime efficiency scaled by jobslot count and virtual cores: {}\n".format(
                        cpu_efficiency_scaled))
                file.write(
                    "Total CPU time / Walltime efficiency scaled by jobslot count and physical cores: {}\n".format(
                        cpu_efficiency_scaled_physical))
                file.write("\n")

                file.write("## Data from other sources (FULL data set):\n")
                file.write(
                    "Total CPU Efficiency from {} to {} (from GridKa perspective, with Pilots): {}".format(start_date,
                                                                                                           end_date,
                                                                                                           cpu_efficiency_data))

    # Remove types that are very infrequent in the data
    # job_data_filtered = jobmonitoring.filter_df_by_type(job_data, 0.0001)

    # job_partitions = jobtypesplit.split_by_type(job_data_filtered, 'Type', True)
    # demand_list = [demandextraction.extract_demands(partition) for partition in job_partitions]
    #
    # for demands in demand_list:
    #     logging.debug("Demands:\n{}".format(demands))

    # demandextraction.extract_demands(job_data)
