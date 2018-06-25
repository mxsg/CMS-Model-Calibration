#!/usr/bin/env python3

import sys
import os

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

def main():
    if len(sys.argv) != 3:
        print("Usage: python utilization.py <directory> <file_string>")
        print("<directory> directory to search for CSV utilization files in")
        print("<file_string> string that must be included in file names to be analyzed")
        quit()

    directory = sys.argv[1]
    name_part = sys.argv[2]

    file_paths = [os.path.join(directory, i) for i in os.listdir(directory) if os.path.isfile(os.path.join(directory,i)) and name_part in i]

    print("Number of paths: {}".format(len(file_paths)))

    total_utilization = average_core_utilization(file_paths)
    print("Total utilization: {}".format(total_utilization))

if __name__ == '__main__':
    main()
