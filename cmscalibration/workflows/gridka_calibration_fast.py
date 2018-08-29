import logging
import os
import pickle
from datetime import datetime

import pandas as pd

from analysis import demandextraction, calibrationreport, resource_usage, cpuefficiency, sampling
from analysis import jobreportanalysis
from analysis import jobreportcleaning
from analysis import nodeanalysis
from data.dataset import Metric
from exporters import references
from exporters.datasetexport import CalibrationParameterExporter
from importers.dataset_import import DatasetImporter
from importers.gridkadata import GridKaNodeDataImporter, CoreUsageImporter, ColumnCoreUsageImporter, \
    CPUEfficiencyReferenceImporter
from importers.jmimport import JMImporter
from importers.jobcounts import JobCountImporter
from importers.wmaimport import SummarizedWMAImporter
from merge import job_node
from merge.merge_datasets import UnionDatasetMerge
from merge.reportmatching import JobReportMatcher
from utils import config, visualization
from utils import report as rp
from utils.report import ReportBuilder


def run():
    report = ReportBuilder(base_path=config.output_directory, filename='calibration-report.md')

    # Todo Move this somewhere else?
    # Log into file with current date and time

    log_path = os.path.join(config.output_directory, 'log')
    os.makedirs(log_path, exist_ok=True)

    now = datetime.now()
    log_name = "logfile_{}.txt".format(now.strftime('%Y-%m-%d_%H-%M-%S'))

    logging.getLogger().addHandler(
        logging.FileHandler(os.path.join(log_path, log_name)))

    report.append('# GridKa Calibration Run')

    time_now = datetime.now().strftime('%Y-%m-%d, %H:%M:%S')
    report.append('at {}'.format(time_now))
    logging.info("Model Calibration run at {}".format(time_now))

    start_date = pd.to_datetime(config.start_date)
    end_date = pd.to_datetime(config.end_date)

    day_count = (end_date - start_date).days

    report.append("")
    report.append("Start date: {}  \nEnd date: {}".format(start_date, end_date))

    # Todo Remove this optimization again!
    # Todo Better error handling

    if config.cache_dir:
        cache_dir = config.cache_dir
    else:
        cache_dir = 'data/cache'

    os.makedirs(cache_dir, exist_ok=True)

    jm_cache = os.path.join(cache_dir, 'jm_dataset.pkl')
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

    wm_cache = os.path.join(cache_dir, 'wm_dataset.pkl')
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
    nodes = nodeanalysis.add_performance_data(nodes, simulated_cores=config.core_simulation_method)

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

    jobslots_from_reports = jobslot_timeseries['totalSlots'].resample('s').pad().resample('H').mean()

    avg_jobslots_reports = jobslots_from_reports.mean()

    fig, axes = calibrationreport.multiple_jobslot_usage(
        {'Extracted from job reports': jobslots_from_reports, 'Allocated to GridKa CMS Pilots': core_df['cms']})
    # Todo Make this generic for the time period
    # axes.set_title('Allocated job slots ({})'.format("May 2018, 31 days"))

    report.add_figure(fig, axes, 'jobslot_usage_reference')

    # ## Visualize number of jobs in calibration report

    report.append("## Number of jobs completed over time")

    job_counts = JobCountImporter().import_file(config.job_counts_reference, start_date, end_date)
    fig, axes = calibrationreport.jobtypes_over_time_df(job_counts, 'date', 'type')
    report.add_figure(fig, axes, 'job_counts_reference', tight_layout=False)

    job_counts_reference_summary = job_counts.groupby('type')['count'].sum().reset_index()
    job_counts_reference_summary.columns = ['type', 'count']

    job_counts_reference_summary['share'] = job_counts_reference_summary['count'] / job_counts_reference_summary[
        'count'].sum()

    report.append("Job throughput from CMS Dashboard:")
    report.append()
    report.append_paragraph(rp.CodeBlock().append(job_counts_reference_summary.to_string()))

    # CPU Efficiencies

    efficiency_reference = CPUEfficiencyReferenceImporter(col='cms', output_column='value').import_file(
        config.cpu_efficiency_reference, start_date, end_date)

    efficiency_timeseries, reports_average = cpuefficiency.calculate_efficiencies(job_data, freq='12h')
    logging.debug("CPU Efficiency Time series:")
    logging.debug(efficiency_timeseries)

    reference = efficiency_reference['value'].resample('12h').mean().rename('reference')
    reference_mean = efficiency_reference['value'].mean()

    from_reports = efficiency_timeseries.rename('measured')

    # cpu_eff = pd.concat([reference, from_reports], axis=1)

    fig, axes = visualization.draw_efficiency_timeseries(
        {'extracted from job reports': from_reports, 'reference from GridKa monitoring': reference})
    axes.set_ylabel("CPU Efficiency (CPU Time / Walltime)")

    axes.legend(['Extracted from job reports (average {:.2f}%)'.format(reports_average * 100),
                 'Reference from GridKa monitoring (average {:.2f}%)'.format(reference_mean * 100)])

    axes.set_title('CPU Efficiencies (May 2018, 31 days)')

    axes.set_xlim(right=(end_date - pd.Timedelta('1 days')))

    # # %Y
    # date_format = mdates.DateFormatter('%d %b')
    # axes.xaxis.set_major_formatter(date_format)
    # fig.autofmt_xdate()

    # axes.xaxis.set_major_locator(mdates.WeekdayLocator())
    # set major ticks format
    # axes.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

    report.add_figure(fig, axes, 'cpu_efficiencies_reference')

    # Compute calibration parameters
    node_types = nodeanalysis.extract_node_types(nodes)

    # Scale the resource environment with both information from the job reports and the Pilot jobs
    scaled_nodes_pilots = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores)
    scaled_nodes_reports = nodeanalysis.scale_site_by_jobslots(node_types, avg_jobslots_reports)

    job_counts_reference_summary['throughput_day'] = job_counts_reference_summary['count'].divide(day_count)

    # Todo Remove again?
    # type_split_cols = [Metric.JOB_TYPE.value, Metric.USED_CORES.value]
    type_split_cols = [Metric.JOB_TYPE.value]

    job_groups = demandextraction.split_and_filter(job_data, type_split_cols)

    # split_key = 'reprocessing1.0'
    # demandextraction.split_group_by_value(job_groups, split_key, Metric.CPU_IDLE_TIME_RATIO.value, value=0.6)

    demands, partitions = demandextraction.extract_job_demands(job_groups, report,
                                                               equal_width=False,
                                                               drop_overflow=False, bin_count=60, cutoff_quantile=0.95,
                                                               overflow_agg=config.overflow_aggregation_method)

    export_parameters('parameters_slots_from_pilots', scaled_nodes_pilots, demands)
    export_parameters('parameters_slots_from_reports', scaled_nodes_reports, demands)

    # Sample half of the reports, fix random state for reproducibility
    reports_train, reports_test = sampling.split_samples(job_data, frac=0.5, random_state=38728)

    sampling_report = ReportBuilder(base_path=config.output_directory, filename='sampled-calibration-report.md',
                                    resource_dir='sampling-figures')

    job_groups_train = demandextraction.split_and_filter(reports_train, type_split_cols)

    # split_key = 'reprocessing1.0'
    # demandextraction.split_group_by_value(job_groups_train, split_key, Metric.CPU_IDLE_TIME_RATIO.value, value=0.6)

    sample_demands, sample_partitions = demandextraction.extract_job_demands(job_groups_train, sampling_report,
                                                                             equal_width=False,
                                                                             drop_overflow=False, bin_count=60,
                                                                             cutoff_quantile=0.95,
                                                                             overflow_agg=config.overflow_aggregation_method)
    sampling_report.write()

    export_parameters('parameters_slots_from_pilots_sampled0.5', scaled_nodes_pilots, sample_demands)
    export_parameters('parameters_slots_from_reports_sampled0.5', scaled_nodes_reports, sample_demands)

    # Export job throughputs from analyzed jobs

    jobs_from_reports = job_data.copy()
    jobs_from_reports[Metric.JOB_TYPE.value] = jobs_from_reports[Metric.JOB_TYPE.value].fillna('unknown')
    job_counts_reports = jobs_from_reports.groupby(Metric.JOB_TYPE.value).size().reset_index()
    job_counts_reports.columns = ['type', 'count']
    job_counts_reports['throughput_day'] = job_counts_reports['count'].divide(day_count)

    export_job_counts(job_counts_reports, 'parameters_slots_from_pilots',
                      'job_counts_reference_extracted_reports.csv')

    # Export walltimes
    references.export_walltimes(partitions, os.path.join(config.output_directory, 'parameters_slots_from_pilots',
                                                         'job_walltimes_references.csv'))

    # Export job type shares

    # Write jobs to report
    calibrationreport.add_jobs_report_section(jm_dataset, report)

    # Write report out to disk
    report.write()


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
