import logging
import os

from datetime import datetime
import numpy as np
import pandas as pd

import analysis.jobreportanalysis
from analysis import demandextraction, visualization
from analysis import jobreportanalysis
from analysis import nodeanalysis
from analysis import sampling
from exporters import demandexport
from exporters.nodetypes import NodeTypeExporter
from importers.gridkadata import CPUEfficienciesImporter, GridKaNodeDataImporter, CoreUsageImporter
from importers.dataset_import import DatasetImporter
from importers.jmimport import JMImporter
from importers.wmaimport import SummarizedWMAImporter
from data.dataset import Metric
from merge import job_node
from utils import config
from validation import cpuefficiency
from utils.report import ReportBuilder


def run():

    report = ReportBuilder(base_path=config.output_directory, filename='calibration-report.md')

    report.append('# GridKa Calibration Run')
    report.append('at {}'.format(datetime.now().strftime('%Y-%m-%d, %H:%M:%S')))

    start_date = pd.to_datetime(config.start_date)
    end_date = pd.to_datetime(config.end_date)

    report.append("")
    report.append("Start date: {}  \nEnd date: {}".format(start_date, end_date))

    # Timezone correction correct for errors in timestamps of JobMonitoring data
    dataset_importer = DatasetImporter(
        JMImporter(timezone_correction='Europe/Berlin', hostname_suffix='.gridka.de', with_files=False))
    jm_dataset = dataset_importer.import_dataset(config.jm_input_dataset, start_date, end_date)

    wm_dataset = DatasetImporter(SummarizedWMAImporter(with_files=False)) \
        .import_dataset(config.wm_input_dataset, start_date, end_date)


    # jobs = jm_importer.importDataFromFile('./data/output_jobmonitoring_2018-03to04.txt')
    jobs = jm_dataset.df

    nodes = GridKaNodeDataImporter().import_file('./data/gridka-benchmarks-2017.csv')
    nodeanalysis.add_performance_data(nodes)

    matched_jobs = job_node.match_jobs_to_node(jm_dataset.df, nodes)

    job_data = jobreportanalysis.add_jobmonitoring_performance_data(matched_jobs)



    cpu_efficiency = cpuefficiency.cpu_efficiency(job_data)
    logging.info("Total CPU time / Walltime efficiency: {}".format(cpu_efficiency))
    cpu_efficiency_scaled = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_data)
    logging.info("Total CPU time / Walltime efficiency scaled by jobslot count and virtual cores: {}".format(
        cpu_efficiency_scaled))

    cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_data, physical=True)
    logging.info("Total CPU time / Walltime efficiency scaled by jobslot count and physical cores: {}".format(
        cpu_efficiency_scaled_physical))

    core_importer = CoreUsageImporter()
    core_usage_cms = core_importer.import_file('./data/core_usage_data_cms.csv', config.start_date, config.end_date)
    total_cores = core_importer.import_file('./data/total_available_cores.csv', config.start_date, config.end_date)

    core_usage_cms = core_usage_cms.rename(columns={'Value': 'CMSCores'})
    total_cores = total_cores.rename(columns={'Value': 'TotalCores'})

    logging.debug(core_usage_cms.columns)

    core_df = core_usage_cms[['Timestamp', 'CMSCores']].merge(total_cores[['Timestamp', 'TotalCores']], on='Timestamp')
    core_df['CMSShare'] = core_df['CMSCores'] / core_df['TotalCores']
    core_df[core_df['CMSShare'] > 1] = np.nan

    logging.info("Total mean share for CMS: {}".format(core_df['CMSShare'].mean()))
    core_df_timeperiod = core_df[(core_df['Timestamp'] <= end_date) & (core_df['Timestamp'] >= start_date)]

    mean_core_share_in_timeframe = core_df_timeperiod.mean()
    logging.info(
        "Mean share for CMS in time frame from {} to {}: {}".format(start_date, end_date, mean_core_share_in_timeframe))

    cms_avg_cores = mean_core_share_in_timeframe['CMSCores']

    logging.info("Mean number of slots for CMS: {}".format(cms_avg_cores))




    node_types = nodeanalysis.extract_node_types(nodes)
    scaled_nodes = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    cpu_eff_importer = CPUEfficienciesImporter()
    cpu_eff_df = cpu_eff_importer.import_file('./data/gridka_cpu_over_walltime.csv', config.start_date, config.end_date)

    cpu_efficiency_data = analysis.jobreportanalysis.compute_average_cpu_efficiency(cpu_eff_df, start=start_date,
                                                                                    end=end_date)
    # cpu_efficiency_data = cpuefficiencyanalysis.compute_average_cpu_efficiency(cpu_eff_df)
    logging.debug(
        "CPU Efficiency total from {} to {} (from GridKa perspective, with Pilots): {}".format(start_date, end_date,
                                                                                               cpu_efficiency_data))

    out_parent = './out/params'

    # Create full parameter set
    out_directory = os.path.join(out_parent, "full")
    info_file = 'info.txt'

    logging.debug("===== Job count before dropping: {}".format(job_data.shape[0]))

    job_subset = job_data[(job_data[Metric.STOP_TIME.value] >= start_date) &
                          (job_data[Metric.FINISHED_TIME.value] < end_date)].copy()

    # job_subset = job_subset.drop_duplicates(['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp'])

    logging.debug("===== Job count after dropping: {}".format(job_subset.shape[0]))

    filename = os.path.join(out_directory, 'nodes.json')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    NodeTypeExporter().export_to_json_file(scaled_nodes, filename)

    demands = demandextraction.extract_job_demands(job_subset)


    # Write jobs to report
    perf_jobs_dataset = jm_dataset
    perf_jobs_dataset.df = job_subset

    visualization.add_jobs_report_section(perf_jobs_dataset, report)

    # TODO Refactor this into its own method!
    day_count = (end_date - start_date).days

    summary = job_subset.groupby(Metric.JOB_TYPE.value).size().reset_index()
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
        cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_subset, physical=True)

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

    sample_shares = [0.5]

    for sample_share in sample_shares:
        out_share_dir = os.path.join(out_parent, "share_{}".format(sample_share))

        samples_subdirs = ['sample1', 'sample2']
        info_file = 'info.txt'

        logging.debug("===== Job count before dropping: {}".format(job_data.shape[0]))

        job_subset = job_data[(job_data[Metric.START_TIME.value] >= start_date) &
                              (job_data[Metric.FINISHED_TIME.value] < end_date)].copy()

        # job_subset = job_subset.drop_duplicates(['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp'])

        logging.debug("===== Job count after dropping: {}".format(job_subset.shape[0]))

        samples = sampling.split_samples(job_subset, frac=sample_share)

        for i, sample in enumerate(samples):
            out_subdirectory = os.path.join(out_share_dir, samples_subdirs[i])
            filename = os.path.join(out_subdirectory, 'nodes.json')
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            NodeTypeExporter().export_to_json_file(scaled_nodes, filename)

            demands = demandextraction.extract_job_demands(sample)

            # TODO Refactor this into its own method!
            day_count = (end_date - start_date).days

            summary = sample.groupby(Metric.JOB_TYPE.value).size().reset_index()
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
                cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots(job_subset, physical=True)

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
                cpu_efficiency_scaled_physical = cpuefficiency.cpu_efficiency_scaled_by_jobslots(sample, physical=True)
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

    # Write report out to disk
    report.write()
