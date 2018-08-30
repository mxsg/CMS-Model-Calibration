import logging
import os
import pickle
from datetime import datetime

import pandas as pd

from analysis import calibrationreport, resource_usage, cpuefficiency, sampling
from analysis import jobreportanalysis
from analysis import jobreportcleaning
from analysis import nodeanalysis
from analysis.demandextraction import FilteredJobClassifier, JobDemandExtractor
from data.dataset import Metric
from exporters.datasetexport import ReferenceWalltimeExporter
from importers.dataset_import import DatasetImporter
from importers.gridkadata import GridKaNodeDataImporter, ColumnCoreUsageImporter, \
    CPUEfficiencyReferenceImporter
from importers.jmimport import JMImporter
from importers.jobcounts import JobCountImporter
from importers.wmaimport import SummarizedWMAImporter
from interfaces.workflow import CalibrationWorkflow
from merge import job_node
from merge.merge_datasets import UnionDatasetMerge
from merge.reportmatching import JobReportMatcher
from utils import config, visualization
from utils import report as rp
from utils.report import ReportBuilder
from workflows.workflowutils import export_job_counts, export_parameters


# Todo Split this up into smaller methods

class GridKaCalibration(CalibrationWorkflow):

    def __init__(self):
        self.report = ReportBuilder(base_path=config.outputDirectory, filename='calibration-report.md')

    def run(self):

        self.report.append('# GridKa Calibration Run')

        time_now = datetime.now().strftime('%Y-%m-%d, %H:%M:%S')
        self.report.append('at {}'.format(time_now))
        logging.info("Model Calibration run at {}".format(time_now))

        start_date = pd.to_datetime(config.startDate)
        end_date = pd.to_datetime(config.endDate)

        day_count = (end_date - start_date).days

        self.report.append()
        self.report.append("Start date: {}  \nEnd date: {}".format(start_date, end_date))

        # Import data sets
        ##################

        # Timezone correction correct for errors in timestamps of JobMonitoring data
        dataset_importer = DatasetImporter(
            JMImporter(timezone_correction='Europe/Berlin', hostname_suffix='.gridka.de', with_files=False))
        jm_dataset = dataset_importer.import_dataset(config.inputPaths['jm'], start_date, end_date)

        wm_dataset = DatasetImporter(SummarizedWMAImporter(with_files=False)) \
            .import_dataset(config.inputPaths['wma'], start_date, end_date)

        cached_matches = None
        use_caching = config.cacheDir is not None

        if use_caching:
            match_cache_file = os.path.join(config.cacheDir, 'jm-wma-matches.csv')

            if os.path.isfile(match_cache_file):
                try:
                    cached_matches = pd.read_csv(match_cache_file,
                                                 usecols=[jm_dataset.df.index.name, wm_dataset.df.index.name])
                    logging.info(
                        "Loaded {} matches from match cache {}!".format(cached_matches.shape[0], match_cache_file))
                except Exception:
                    logging.warning("No match cache found at {}!".format(match_cache_file))

        # Match Jobmonitoring and WMArchive job reports
        matcher = JobReportMatcher(timestamp_tolerance=10, time_grouping_freq='D')
        matches = matcher.match_reports(jm_dataset, wm_dataset, use_files=False, previous_matches=cached_matches)

        if use_caching:
            match_cache_file = os.path.join(config.cacheDir, 'jm-wma-matches.csv')

            logging.info("Writing {} matches to file {}".format(matches.shape[0], match_cache_file))
            matches.to_csv(match_cache_file)

        jobs_dataset = UnionDatasetMerge().merge_datasets(matches, jm_dataset, wm_dataset, left_index='UniqueID',
                                                          right_index='wmaid', left_suffix='jm', right_suffix='wma')

        jobs_dataset.df = jobreportcleaning.clean_job_reports(jobs_dataset.df)

        # Import node information
        nodes = GridKaNodeDataImporter().import_file(config.inputPaths['nodeInfo'])
        nodes = nodeanalysis.add_performance_data(nodes, simulated_cores=config.workflowOptions['coreSimulationMethod'])

        # Match jobs to nodes
        matched_jobs = job_node.match_jobs_to_node(jobs_dataset.df, nodes)
        matched_jobs = jobreportanalysis.add_missing_node_info(matched_jobs, nodes)

        jm_dataset.df = jobreportanalysis.add_performance_data(matched_jobs)
        job_data = jm_dataset.df

        # Import additional information for usage of GridKa site
        core_importer = ColumnCoreUsageImporter()
        core_df = core_importer.import_file(config.inputPaths['coreUsage'], start_date, end_date)
        cms_avg_cores = core_df['cms'].mean()

        avg_jobslots_reports = self.draw_jobslot_usage(jm_dataset, core_df)

        # Visualize number of jobs in calibration report
        job_counts_reference_summary = self.add_jobs_over_time(start_date, end_date)

        # CPU Efficiencies
        self.add_cpu_efficiency(job_data, start_date, end_date)

        # Compute calibration parameters
        node_types = nodeanalysis.extract_node_types(nodes)

        # Scale the resource environment with both information from the job reports and the Pilot jobs
        scaled_nodes_pilots = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores)
        scaled_nodes_reports = nodeanalysis.scale_site_by_jobslots(node_types, avg_jobslots_reports)

        type_split_cols = config.workflowOptions['typeSplitCols']

        split_types = None
        if 'splitTypes' in config.workflowOptions:
            split_types = list(map(tuple, config.workflowOptions['splitTypes']))

        job_classifier = FilteredJobClassifier(type_split_cols, split_types=split_types)
        job_groups = job_classifier.split(job_data)

        job_demand_extractor = JobDemandExtractor(self.report, equal_width=False, drop_overflow=False, bin_count=60,
                                                  cutoff_quantile=0.95,
                                                  overflow_agg=config.workflowOptions['overflowAggregationMethod'])

        demands, partitions = job_demand_extractor.extract_job_demands(job_groups)

        export_parameters('parameters_slots_from_pilots', scaled_nodes_pilots, demands)
        export_parameters('parameters_slots_from_reports', scaled_nodes_reports, demands)

        # Sample half of the reports, fix random state for reproducibility
        reports_train, reports_test = sampling.split_samples(job_data, frac=0.5, random_state=38728)

        sampling_report = ReportBuilder(base_path=config.outputDirectory, filename='calibration-report-sampled.md',
                                        resource_dir='figures-sampling')

        job_groups_train = job_classifier.split(reports_train)

        job_demand_extractor.report = sampling_report
        sample_demands, sample_partitions = job_demand_extractor.extract_job_demands(job_groups_train)

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
                          config.outputPaths['jobCountReports'])

        # Export walltimes
        walltime_path = os.path.join(config.outputDirectory, 'parameters_slots_from_pilots',
                                     config.outputPaths['walltimeReference'])
        ReferenceWalltimeExporter().export_to_json_file(partitions, walltime_path)

        # Write jobs to report
        calibrationreport.add_jobs_report_section(jm_dataset, self.report)

        # Write report out to disk
        self.report.write()

    def draw_jobslot_usage(self, jm_dataset, core_reference):

        jobslot_timeseries = resource_usage.calculate_jobslot_usage(jm_dataset.df, jm_dataset.start, jm_dataset.end,
                                                                    start_ts_col=Metric.START_TIME.value,
                                                                    end_ts_col=Metric.STOP_TIME.value,
                                                                    slot_col=Metric.USED_CORES.value)

        jobslots_from_reports = jobslot_timeseries['totalSlots'].resample('s').pad().resample('H').mean()
        avg_jobslots_reports = jobslots_from_reports.mean()

        fig, axes = calibrationreport.multiple_jobslot_usage(
            {'Extracted from job reports': jobslots_from_reports,
             'Allocated to GridKa CMS Pilots': core_reference['cms']})

        self.report.add_figure(fig, axes, 'jobslot_usage_reference')

        return avg_jobslots_reports

    def add_jobs_over_time(self, start_date, end_date):
        self.report.append("## Number of jobs completed over time")

        job_counts = JobCountImporter().import_file(config.inputPaths['jobCountsReference'], start_date, end_date)
        fig, axes = calibrationreport.jobtypes_over_time_df(job_counts, 'date', 'type')
        self.report.add_figure(fig, axes, 'job_counts_reference', tight_layout=False)

        job_counts_reference_summary = job_counts.groupby('type')['count'].sum().reset_index()
        job_counts_reference_summary.columns = ['type', 'count']

        job_counts_reference_summary['share'] = job_counts_reference_summary['count'] / job_counts_reference_summary[
            'count'].sum()
        job_counts_reference_summary['throughput_day'] = job_counts_reference_summary['count'].divide(
            (end_date - start_date).days)

        self.report.append("Job throughput from CMS Dashboard:")
        self.report.append()
        self.report.append_paragraph(rp.CodeBlock().append(job_counts_reference_summary.to_string()))

        return job_counts_reference_summary

    def add_cpu_efficiency(self, job_data, start_date, end_date):
        efficiency_reference = CPUEfficiencyReferenceImporter(col='cms', output_column='value').import_file(
            config.inputPaths['CPUEfficiencyReference'], start_date, end_date)

        efficiency_timeseries, reports_average = cpuefficiency.calculate_efficiencies(job_data, freq='12h')

        reference = efficiency_reference['value'].resample('12h').mean().rename('reference')
        reference_mean = efficiency_reference['value'].mean()

        from_reports = efficiency_timeseries.rename('measured')

        # cpu_eff = pd.concat([reference, from_reports], axis=1)

        fig, axes = visualization.draw_efficiency_timeseries(
            {'extracted from job reports': from_reports, 'reference from GridKa monitoring': reference})
        axes.set_ylabel("CPU Efficiency (CPU Time / Walltime)")

        axes.legend(['Extracted from job reports (average {:.2f}%)'.format(reports_average * 100),
                     'Reference from GridKa monitoring (average {:.2f}%)'.format(reference_mean * 100)])

        axes.set_title("CPU Efficiencies ({}, {} days)".format(config.runName, (end_date - start_date).days))

        axes.set_xlim(right=(end_date - pd.Timedelta('1 days')))

        self.report.add_figure(fig, axes, 'cpu_efficiencies_reference')
