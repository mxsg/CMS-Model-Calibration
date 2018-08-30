#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import traceback
from datetime import datetime

import numpy as np
import pandas as pd

from interfaces.workflow import CalibrationWorkflow
from utils import config


def main():
    """Main function for the model calibration tool."""

    parser = argparse.ArgumentParser("Extract calibration parameters from CMS monitoring data.")
    parser.add_argument("--conf", default="calibration.json",
                        help="Path to the configuration file used for calibration")

    args = parser.parse_args()

    # Load configuration file
    try:
        config.load_config(args.conf)
    except ValueError as e:
        # Directly print as logging infrastructure is not set up yet
        print("Could not load configuration file. Error: {}".format(e))
        print("Exiting.")
        sys.exit(1)

    log_subdir = os.path.join(config.output_directory, 'log')
    setup_logging(log_subdir)
    logging.getLogger().setLevel(logging.DEBUG)

    logging.info("Starting Model Calibration.")

    logging.debug("Running with Pandas version: {}".format(pd.__version__))
    logging.debug("Running with Numpy version: {}".format(np.__version__))

    # Load workflow module from configuration file
    def import_class(name):
        modname, classname = name.rsplit('.', 1)
        module = __import__(modname, fromlist=[classname])
        attr = getattr(module, classname)
        return attr

    try:
        workflow_class = import_class(config.workflow)
    except ImportError as e:
        logging.error("Could not import workflow class {}. Error: {}".format(config.workflow, str(e)))
        sys.exit(1)

    if not issubclass(workflow_class, CalibrationWorkflow):
        logging.error("Workflows must implement the CalibrationWorkflow interface.")
        sys.exit(1)

    # Run workflow
    workflow = workflow_class()
    try:
        workflow.run()
    except Exception:
        logging.error("An error occured during execution of the workflow!")
        logging.error(traceback.format_exc())
        sys.exit(1)

    logging.info("Model Calibration Finished")


def setup_logging(log_path: str):
    """Setup the logging files at the specified path."""

    # Setup logging to file and the console
    os.makedirs(log_path, exist_ok=True)

    # Log into file with current date and time
    now = datetime.now()
    log_name = "logfile_{}.txt".format(now.strftime('%Y-%m-%d_%H-%M-%S'))

    logger = logging.getLogger()
    logger.handlers = []

    filehandler = logging.FileHandler(os.path.join(log_path, log_name))
    streamhandler = logging.StreamHandler(stream=sys.stdout)

    formatter = logging.Formatter('%(asctime)s [%(levelname)-5.5s]  %(message)s')
    filehandler.setFormatter(formatter)
    streamhandler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(filehandler)
    logger.addHandler(streamhandler)


if __name__ == '__main__':
    main()
