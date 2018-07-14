#!/usr/bin/env python3

import logging
import os
import sys
from datetime import datetime
import importlib

import numpy as np
import pandas as pd
from utils import config
from workflows import gridka_calibration


def main():
    setup_logging()

    conf_path = 'calibration.json'
    param_count = len(sys.argv) - 1

    # Check for supplied configuration file or standard configuration file location
    if param_count > 1:
        print("Please provide a single configuration file path.")
        print("You can also supply a configuration in the standard location '{}'".format(conf_path))
        sys.exit(1)

    if param_count == 1:
        conf_path = sys.argv[1]

    # Load configuration file
    try:
        config.load_config(conf_path)
    except ValueError as e:
        print("Could not load configuration file. Error: {}".format(e))
        print("Exiting.")
        sys.exit(1)

    logging.debug("Starting Model Calibration.")

    logging.debug("Running with Pandas version: {}".format(pd.__version__))
    logging.debug("Running with Numpy version: {}".format(np.__version__))

    # Load workflow module from configuration file
    try:
        workflow_module = importlib.import_module(config.workflow_module)
    except ImportError as e:
        print("Could not import workflow module {}. Error: {}".format(config.workflow_module, str(e)))
        sys.exit(1)

    if not hasattr(workflow_module, 'run'):
        print("Could not run workflow. Workflows must implement a run() method.")
        sys.exit(1)
    else:
        workflow_module.run()

    logging.debug("Model Calibration Finished")


def setup_logging():
    # Setup logging to file and the console
    log_path = "log"

    # Log into file with current date and time
    now = datetime.now()
    log_name = "logfile_{}".format(now.strftime('%Y-%m-%d_%H-%M-%S'))
    log_path = os.path.join(log_path, log_name)
    os.makedirs(log_path, exist_ok=True)

    logging.basicConfig(
        format='%(asctime)s [%(levelname)-5.5s]  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(os.path.join(log_path, log_name)),
            logging.StreamHandler(stream=sys.stdout)
        ],
        level=logging.DEBUG)


if __name__ == '__main__':
    main()
