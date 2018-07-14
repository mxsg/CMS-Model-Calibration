"""
Configuration management module. Loads JSON configuration files and provides
values in its global namespace.
"""

import json
import logging
import os


def load_config(config_path):
    """ Load a configuration file from JSON. """
    logging.debug("Loading configuration from path {}".format(config_path))

    if not os.path.isfile(config_path):
        logging.warning("Could not find configuration file in path {}".format(config_path))
        raise ValueError("Configuration file not found.")

    def load_key(key, dictionary, default=None):
        if key not in dictionary:
            raise ValueError(f"Could not find required key {key}!")
        globals()[key] = dictionary.get(key, default)

    with open(config_path) as file:
        config = json.load(file)

    # Load required configuration keys

    load_key('start_date', config)
    load_key('end_date', config)

    load_key('wm_input_dataset', config)
    load_key('jm_input_dataset', config)
    load_key('node_info', config)
    load_key('workflow_module', config)
