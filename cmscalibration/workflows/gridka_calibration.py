import os
from datetime import datetime

import pandas as pd

from analysis import demandextraction, calibrationreport
from analysis import jobreportanalysis
from analysis import nodeanalysis
from exporters.datasetexport import CalibrationParameterExporter
from importers.dataset_import import DatasetImporter
from importers.gridkadata import GridKaNodeDataImporter, CoreUsageImporter
from importers.jmimport import JMImporter
from importers.wmaimport import SummarizedWMAImporter
from merge import job_node
from merge.merge_datasets import UnionDatasetMerge
from merge.reportmatching import JobReportMatcher
from utils import config
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

    # Match Jobmonitoring and WMArchive job reports
    matcher = JobReportMatcher(timestamp_tolerance=10, time_grouping_freq='D')
    matches = matcher.match_reports(jm_dataset, wm_dataset, use_files=False)

    # Merge datasets using the augment strategy, i.e. using values from the second dataset to replace missing
    # values in the first.
    jobs_dataset = UnionDatasetMerge().merge_datasets(matches, jm_dataset, wm_dataset, left_index='UniqueID',
                                                      right_index='wmaid', left_suffix='jm', right_suffix='wma')

    # Import node information
    nodes = GridKaNodeDataImporter().import_file(config.node_info)
    nodes = nodeanalysis.add_performance_data(nodes)

    # Match jobs to nodes
    matched_jobs = job_node.match_jobs_to_node(jobs_dataset.df, nodes)

    jm_dataset.df = jobreportanalysis.add_performance_data(matched_jobs)
    job_data = jm_dataset.df

    # Import additional information for usage of GridKa site
    core_importer = CoreUsageImporter()
    core_df = core_importer.import_core_share('data/total_available_cores.csv', 'data/core_usage_data_cms.csv',
                                              start_date, end_date,
                                              share_col='CMSShare', partial_col='CMSCores', total_col='TotalCores')

    # Compute the average number of cores occupied by CMS in the considered time frame
    cms_avg_cores = core_df['CMSCores'].mean()

    # Compute calibration parameters
    node_types = nodeanalysis.extract_node_types(nodes)
    scaled_nodes = nodeanalysis.scale_site_by_jobslots(node_types, cms_avg_cores)

    demands = demandextraction.extract_job_demands(job_data)

    parameter_path = os.path.join(config.output_directory, 'parameters')

    exporter = CalibrationParameterExporter(parameter_path)
    exporter.export(scaled_nodes, 'nodes.json', demands, 'jobs.json')

    # Write jobs to report
    calibrationreport.add_jobs_report_section(jm_dataset, report)

    # Write report out to disk
    report.write()
