import logging
import os
import pickle
from datetime import datetime

import pandas as pd

from analysis import demandextraction, calibrationreport, resource_usage
from analysis import jobreportanalysis
from analysis import jobreportcleaning
from analysis import nodeanalysis
from data.dataset import Metric
from exporters.datasetexport import CalibrationParameterExporter
from importers.dataset_import import DatasetImporter
from importers.gridkadata import GridKaNodeDataImporter, CoreUsageImporter, ColumnCoreUsageImporter
from importers.jmimport import JMImporter
from importers.jobcounts import JobCountImporter
from importers.wmaimport import SummarizedWMAImporter
from merge import job_node
from merge.merge_datasets import UnionDatasetMerge
from merge.reportmatching import JobReportMatcher
from utils import config
from utils import report as rp
from utils.report import ReportBuilder


def run():
    report = ReportBuilder(base_path=config.output_directory, filename='calibration-report.md')

    report.append('# GridKa Calibration Run')
    report.append('at {}'.format(datetime.now().strftime('%Y-%m-%d, %H:%M:%S')))

    start_date = pd.to_datetime(config.start_date)
    end_date = pd.to_datetime(config.end_date)

    report.append("")
    report.append("Start date: {}  \nEnd date: {}".format(start_date, end_date))

    # Todo Remove this optimization again!
    # Todo Better error handling

    jm_cache = 'data/cache/jm_dataset.pkl'
    jm_dataset = None

    try:
        with open(jm_cache, 'rb') as file:
            jm_dataset = pickle.load(file)
            logging.info("Loaded {} jobs from jm dataset at {}.".format(jm_dataset.df.shape[0], jm_cache))
    except (IOError, pickle.UnpicklingError) as e:
        logging.info("Could not load jobs from {}".format(jm_cache))
        # Timezone correction correct for errors in timestamps of JobMonitoring data
        dataset_importer = DatasetImporter(
            JMImporter(timezone_correction='Europe/Berlin', hostname_suffix='.gridka.de', with_files=False))
        jm_dataset = dataset_importer.import_dataset(config.jm_input_dataset, start_date, end_date)

        with open(jm_cache, 'wb') as file:
            pickle.dump(jm_dataset, file)
            logging.info("Exported Jobmonitoring data to {}".format(jm_cache))

    wm_cache = 'data/cache/wm_dataset.pkl'
    wm_dataset = None

    try:
        with open(wm_cache, 'rb') as file:
            wm_dataset = pickle.load(file)
            logging.info("Loaded {} jobs from jm dataset at {}.".format(wm_dataset.df.shape[0], wm_cache))

    except (IOError, pickle.UnpicklingError) as e:
        logging.info("Could not load jobs from {}".format(wm_cache))
        # Timezone correction correct for errors in timestamps of JobMonitoring data
        wm_dataset = DatasetImporter(SummarizedWMAImporter(with_files=False)) \
            .import_dataset(config.wm_input_dataset, start_date, end_date)

        with open(wm_cache, 'wb') as file:
            pickle.dump(wm_dataset, file)
            logging.info("Exported WMArchive data to {}".format(wm_cache))

    # Todo Make this more generic!
    match_cache_file = 'data/jm-wmarchive-matches.csv'
    cached_matches = None

    if os.path.isfile(match_cache_file):
        try:
            cached_matches = pd.read_csv(match_cache_file, usecols=[jm_dataset.df.index.name, wm_dataset.df.index.name])
            logging.info("Loaded {} matches from match cache {}!".format(cached_matches.shape[0], match_cache_file))
        except:
            logging.info("No match cache found at {}!".format(match_cache_file))

    # Match Jobmonitoring and WMArchive job reports
    matcher = JobReportMatcher(timestamp_tolerance=10, time_grouping_freq='D')
    matches = matcher.match_reports(jm_dataset, wm_dataset, use_files=False, previous_matches=cached_matches)

    logging.info("Writing {} matches to file {}".format(matches.shape[0], match_cache_file))
    matches.to_csv(match_cache_file)

    # Merge datasets using the augment strategy, i.e. using values from the second dataset to replace missing
    # values in the first.
    jobs_dataset = UnionDatasetMerge().merge_datasets(matches, jm_dataset, wm_dataset, left_index='UniqueID',
                                                      right_index='wmaid', left_suffix='jm', right_suffix='wma')

    jobs_dataset.df = jobreportcleaning.clean_job_reports(jobs_dataset.df)

    # Import node information
    nodes = GridKaNodeDataImporter().import_file(config.node_info)
    nodes = nodeanalysis.add_performance_data(nodes)

    # Match jobs to nodes
    matched_jobs = job_node.match_jobs_to_node(jobs_dataset.df, nodes)

    logging.debug("Nodes with columns {}".format(nodes.columns))
    matched_jobs = jobreportanalysis.add_missing_node_info(matched_jobs, nodes)

    jm_dataset.df = jobreportanalysis.add_performance_data(matched_jobs)
    job_data = jm_dataset.df

    # Import additional information for usage of GridKa site
    core_importer = CoreUsageImporter()
    core_df = core_importer.import_core_share('data/total_available_cores.csv', 'data/core_usage_data_cms.csv',
                                              start_date, end_date,
                                              share_col='CMSShare', partial_col='CMSCores', total_col='TotalCores')

    # Compute the average number of cores occupied by CMS in the considered time frame
    cms_avg_cores = core_df['CMSCores'].mean()
    logging.debug("Average cores used (6h): {}".format(cms_avg_cores))

    core_importer = ColumnCoreUsageImporter()
    core_df = core_importer.import_file('data/gridka_core_usage.csv', start_date, end_date)

    cms_avg_cores = core_df['cms'].mean()
    logging.debug("Average cores used (20m): {}".format(cms_avg_cores))

    jobslot_timeseries = resource_usage.calculate_jobslot_usage(jm_dataset.df, jm_dataset.start, jm_dataset.end,
                                                                start_ts_col=Metric.START_TIME.value,
                                                                end_ts_col=Metric.STOP_TIME.value,
                                                                slot_col=Metric.USED_CORES.value)

    joblslots_from_reports = jobslot_timeseries['totalSlots'].resample('s').pad().resample('H').mean()

    fig, axes = calibrationreport.multiple_jobslot_usage(
        {'from job reports': joblslots_from_reports, 'from GridKa usage data': core_df['cms']})

    report.add_figure(fig, axes, 'jobslot_usage_reference')

    # ## Visualize number of jobs in calibration report

    report.append("## Number of jobs completed over time")

    job_counts = JobCountImporter().import_file('data/siteActivitycsv-may.csv', start_date, end_date)
    fig, axes = calibrationreport.jobtypes_over_time_df(job_counts, 'date', 'count')
    report.add_figure(fig, axes, 'job_counts_reference', tight_layout=False)

    job_counts_reference_summary = job_counts.groupby('type')['count'].sum().reset_index()
    job_counts_reference_summary.columns = ['type', 'count']

    job_counts_reference_summary['share'] = job_counts_reference_summary['count'] / job_counts_reference_summary['count'].sum()
    report.append_paragraph(rp.CodeBlock().append(job_counts_reference_summary.to_string()))

    # Compute calibration parameters
    node_types = nodeanalysis.extract_node_types(nodes)

    # Todo Scaled share should be 1 for final tests!
    # Todo This is for test with fewer threads!
    share_scale_factor = 0.25

    scaled_nodes = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores * share_scale_factor)

    demands = demandextraction.extract_job_demands(job_data, report)

    parameter_path = os.path.join(config.output_directory, 'parameters')

    exporter = CalibrationParameterExporter(parameter_path)
    exporter.export(scaled_nodes, 'nodes.json', demands, 'jobs.json')

    # Write jobs to report
    calibrationreport.add_jobs_report_section(jm_dataset, report)

    # Write report out to disk
    report.write()
