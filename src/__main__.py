#!/usr/bin/env python3

import logging
from datetime import datetime
import sys

import pandas as pd
import numpy as np

from workflows import gridka_calibration
from workflows import debug


def main():
    setupLogger()
    logging.debug("Starting Model Calibration")

    logging.debug("Running with Pandas version: {}".format(pd.__version__))
    logging.debug("Running with Numpy version: {}".format(np.__version__))

    debug.run_workflow()

    logging.debug("Model Calibration Finished")


def setupLogger():
    # Setup logging to file and the console
    log_path = "../log"

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