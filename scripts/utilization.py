#!/usr/bin/env python3

# In case this code is run with Python 2
from __future__ import division

import logging
import os
import re
import sys
import datetime

import pandas as pd


def calculate_utilization(csv_path):
    df = pd.read_csv(csv_path, sep=',')

    df.columns = ['time_start', 'state']
    df['time_end'] = df.time_start.shift(-1)
    df['slice_length'] = df.time_end - df.time_start

    total_time = df.iloc[-1].time_start - df.iloc[0].time_start

    # Drop last row with end timestamp
    df = df[:-1]

    busy_time = df[df.state >= 1].slice_length.sum()
    utilization = busy_time / total_time
    # print("Busy time: {}, total length: {}, utilization: {}".format(busy_time, total_time, utilization))
    return utilization


def average_core_utilization(paths):
    utilizations = [calculate_utilization(path) for path in paths]
    return sum(utilizations) / len(utilizations) if len(utilizations) > 0 else 0.0


def count_lines(path, headerlen=0):
    return sum(1 for _ in open(path)) - headerlen


def job_type_throughputs(filenames, directory):
    type_regex = r'jobsSystem.provided_role_system_(.*)\.(.*)\.csv'
    result = []

    pattern = re.compile(type_regex)
    job_types = [(pattern.findall(filename)[0][0], filename) for filename in filenames]

    for type, path in job_types:
        job_throughput = count_lines(os.path.join(directory, path), headerlen=1)
        result.append((type, job_throughput))

    return result


def main():
    if len(sys.argv) != 2:
        print("Usage: python utilization.py <directory> <file_string>")
        print("<directory> directory to search for CSV utilization files in")
        sys.exit(1)

    directory = sys.argv[1]
    output_file = 'results.txt'

    # Logging setup
    logging.basicConfig(
        format='%(message)s',
        # format='%(asctime)s [%(levelname)-5.5s]  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(os.path.join(directory, output_file)),
            logging.StreamHandler(stream=sys.stdout)
        ],
        level=logging.DEBUG)

    logging.info("# Analysis run at {}".format(datetime.datetime.now()))
    logging.info("")

    utilization_keyword = 'State_of_Active_Resource_Tuple'
    overall_throughput_keyword = 'Usage_Scenario'
    type_throughput_keyword = 'provided_role_system'

    # Compute core utilization
    logging.info("## Core Utilizations")

    all_files = [os.path.join(directory, i) for i in os.listdir(directory) if
                 os.path.isfile(os.path.join(directory, i))]

    utilization_file_paths = [path for path in all_files if utilization_keyword in path]

    logging.info("Number of utilization paths: {}".format(len(utilization_file_paths)))

    total_utilization = average_core_utilization(utilization_file_paths)
    logging.info("Total utilization: {}".format(total_utilization))
    logging.info("")

    # Compute throughput
    logging.info("## Throughput")

    total_throughput_paths = [path for path in all_files if overall_throughput_keyword in path]

    if not total_throughput_paths:
        sys.exit("Could not find total utilization file.")

    total_throughput = count_lines(total_throughput_paths[0], headerlen=1)
    logging.info("Total throughput (Usage Scenario): {}".format(total_throughput))
    logging.info("")

    # Compute throughput for single job types
    logging.info("## Throughput Per Type")

    job_throughput_filenames = [i for i in os.listdir(directory) if
                                os.path.isfile(os.path.join(directory, i)) and type_throughput_keyword in i]

    # Sort job types by their name
    throughputs = job_type_throughputs(job_throughput_filenames, directory)
    throughputs.sort(key=lambda x: x[0])

    for type, throughput in throughputs:
        logging.info("Type {}: {} ({} relative share)".format(type, throughput, throughput / total_throughput))


if __name__ == '__main__':
    main()
