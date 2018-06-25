from analysis import nodeanalysis
from exporters import nodetypes
from merge import job_node
from importers.jobmonitoring import JobMonitoringImporter
from importers.nodedata import GridKaNodeDataImporter


def calibrateGridKaModel():
    jm_importer = JobMonitoringImporter()
    jobs = jm_importer.importDataFromFile('./data/jobmonitoring-20180401.txt')

    node_importer = GridKaNodeDataImporter()
    nodes = node_importer.importDataFromFile('./data/gridka-benchmarks-2017.csv')

    nodeanalysis.addPerformanceData(nodes)
    node_types = nodeanalysis.extractNodeTypes(nodes)

    scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.22888333333)
    # scaled_nodes = nodeanalysis.scaleSiteWithNodeTypes(node_types, 0.20)

    nodetypes.exportToJsonFile(scaled_nodes, './out/nodes.json')

    matched_jobs = job_node.match_jobs_to_node(jobs, nodes)

