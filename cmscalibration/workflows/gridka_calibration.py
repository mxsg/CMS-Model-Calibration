from exporters import nodetypes
from importers.jobmonitoring import JobMonitoringImporter
from importers.nodedata import GridKaNodeDataImporter
from merge import job_node
import logging

from analysis import nodeanalysis
from importers.dataset import DatasetImporter
from utils import config


def calibrate_gridka():
    logging.debug("Start calibration for GridKa model.")

    # Timezone correction correct for errors in timestamps of JobMonitoring data
    dataset_importer = DatasetImporter(JobMonitoringImporter(timezone_correction='Europe/Berlin'))

    jm_dataset = dataset_importer.import_dataset(config.jm_input_dataset, config.start_date, config.end_date)
    # jobs = jm_dataset.jobs
    # files = jm_dataset.files

    nodes = GridKaNodeDataImporter().importDataFromFile(config.node_info)

    nodeanalysis.addPerformanceData(nodes)
    node_types = nodeanalysis.extractNodeTypes(nodes)

    scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.22888333333)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    nodetypes.exportToJsonFile(scaled_nodes, './out/nodes.json')

    matched_jobs = job_node.match_jobs_to_node(jobs, nodes)
