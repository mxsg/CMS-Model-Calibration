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

    def load_optional_key(key, dictionary, default=None):
        globals()[key] = dictionary.get(key, default)

    with open(config_path) as file:
        config = json.load(file)

    # Load required configuration keys
    load_key('workflow', config)

    load_key('startDate', config)
    load_key('endDate', config)

    load_key('outputDirectory', config)

    load_optional_key('cacheDir', config)
    load_optional_key('runName', config)

    load_optional_key('workflowOptions', config, default={})
    load_optional_key('inputPaths', config, default={})
    load_optional_key('outputPaths', config, default={})
