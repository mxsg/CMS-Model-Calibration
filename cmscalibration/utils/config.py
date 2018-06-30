import configparser
import os
import logging

def load_config(config_path):

    logging.debug("Loading configuration from path {}".format(config_path))

    if not os.path.isfile(config_path):
        logging.warning("Could not find configuration file in path {}".format(config_path))
        raise ValueError("Configuration file not found.")

    parser = configparser.ConfigParser()
    parser.read(config_path)

    section = 'general'

    globals()['start_date'] = parser.get(section, 'start_date')
    globals()['end_date'] = parser.get(section, 'end_date')
