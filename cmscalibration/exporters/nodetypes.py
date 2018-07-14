import logging
import json

from interfaces.fileexport import JSONExporter


class NodeTypeExporter(JSONExporter):

    def export_to_json_file(self, node_types, path):
        logging.info("Exporting node types to file: {}".format(path))

        cols = ['name', 'cores', 'jobslots', 'computingRate', 'nodeCount']

        df = node_types[cols]
        df = df.sort_values(by=['nodeCount'], ascending=False)

        node_dict = df.to_dict(orient='records')

        with open(path, 'w') as outfile:
            json.dump(node_dict, outfile, indent=4, sort_keys=True)

        logging.info("Finished exporting node types.")


def exportToJsonFile(node_types, path):

    logging.info("Exporting node types to file: {}".format(path))

    cols = ['name', 'cores', 'jobslots', 'computingRate', 'nodeCount']

    df = node_types[cols]
    df = df.sort_values(by=['nodeCount'], ascending=False)

    node_dict = df.to_dict(orient='records')

    # logging.debug("Node types: \n" + json.dumps(node_dict, indent=4, sort_keys=True))

    with open(path, 'w') as outfile:
        json.dump(node_dict, outfile, indent=4, sort_keys=True)

    logging.info("Finished exporting node types.")
