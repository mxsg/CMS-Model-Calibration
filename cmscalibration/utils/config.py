import json
import logging
import os


def load_config(config_path):
    logging.debug("Loading configuration from path {}".format(config_path))

    if not os.path.isfile(config_path):
        logging.warning("Could not find configuration file in path {}".format(config_path))
        raise ValueError("Configuration file not found.")

    def load_key(key, dict, default=None):
        globals()[key] = dict.get(key, default)

    config = json.load(config_path)

    load_key('start_date', config)
    load_key('end_date', config)

    load_key('wma_input_files', config)
    load_key('jm_input_files', config)
