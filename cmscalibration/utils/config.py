import json
import logging
import os

import pandas as pd

# class InputFileSpec:
#     def __init__(self, path, start_date, end_date):
#         self.path = path
#         self.start_date = pd.to_datetime(start_date)
#         self.end_date = pd.to_datetime(end_date)

def load_config(config_path):
    logging.debug("Loading configuration from path {}".format(config_path))

    if not os.path.isfile(config_path):
        logging.warning("Could not find configuration file in path {}".format(config_path))
        raise ValueError("Configuration file not found.")

    def load_key(key, dict, default=None):
        globals()[key] = dict.get(key, default)

    with open(config_path) as file:
        config = json.load(file)

    load_key('start_date', config)
    load_key('end_date', config)

    jm_input = config.get('jm_input_files')
    if jm_input is None:
        jm_input = []
        logging.warn("Configuration: No jm input files found!")


    load_key('wma_input_dataset', config)
    load_key('jm_input_dataset', config)
    load_key('node_info', config)
