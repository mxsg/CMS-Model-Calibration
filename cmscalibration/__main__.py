#!/usr/bin/env python3

import logging
import sys
from datetime import datetime

import numpy as np
import pandas as pd

import configparser

from workflows import debug
from workflows import sampling_validation
from utils import config


def main():

    setupLogger()

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


    logging.debug("Starting Model Calibration")

    logging.debug("Running with Pandas version: {}".format(pd.__version__))
    logging.debug("Running with Numpy version: {}".format(np.__version__))

    sampling_validation.run_workflow()

    logging.debug("Model Calibration Finished")


def setupLogger():
    # Setup logging to file and the console
    log_path = "./log"

    # Log into file with current date and time
    now = datetime.now()
    log_name = "logfile_{}".format(now.strftime('%Y-%m-%d_%H-%M-%S'))

    logging.basicConfig(
        format='%(asctime)s [%(levelname)-5.5s]  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler("{0}/{1}.log".format(log_path, log_name)),
            logging.StreamHandler(stream=sys.stdout)
        ],
        level=logging.DEBUG)


if __name__ == '__main__':
    main()
