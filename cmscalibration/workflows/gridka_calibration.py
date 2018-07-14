import logging

from analysis import nodeanalysis
from exporters import nodetypes
from importers.dataset_import import DatasetImporter
from importers.jmimport import JMImporter
from importers.nodedata import GridKaNodeDataImporter
from importers.wmaimport import SummarizedWMAImporter
from merge.reportmatching import JobReportMatcher
from utils import config


def run():
    logging.debug("Start calibration for GridKa model.")

    # Timezone correction correct for errors in timestamps of JobMonitoring data
    dataset_importer = DatasetImporter(
        JMImporter(timezone_correction='Europe/Berlin', hostname_suffix='.gridka.de', with_files=True))
    jm_dataset = dataset_importer.import_dataset(config.jm_input_dataset, config.start_date, config.end_date)

    wm_dataset = DatasetImporter(SummarizedWMAImporter(with_files=True)) \
        .import_dataset(config.wm_input_dataset, config.start_date, config.end_date)

    matches = JobReportMatcher(timestamp_tolerance=10, time_grouping_freq='D').match_reports(jm_dataset, wm_dataset,
                                                                                             use_files=True)
    # jobs = jm_dataset.jobs
    # files = jm_dataset.files

    nodes = GridKaNodeDataImporter().importDataFromFile(config.node_info)

    nodeanalysis.addPerformanceData(nodes)
    node_types = nodeanalysis.extractNodeTypes(nodes)

    scaled_nodes = nodeanalysis.scale_site_by_benchmark(node_types, 0.22888333333)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    nodetypes.exportToJsonFile(scaled_nodes, './out/nodes.json')

    # matched_jobs = job_node.match_jobs_to_node(jobs, nodes)
