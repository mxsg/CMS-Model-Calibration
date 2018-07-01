#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os
import time
from subprocess import call


def main():
    ssh_setup_commands = ['source ~/analytix/CMSSpark/setup_lxplus.sh']
    ssh_host = 'lxplus7.cern.ch'

    hdfs_base_path = 'hdfs:///cms/users/mstemmer'
    hdfs_dataset = 'wmarchive-monthly'
    start_date = '20180101'
    day_count = 61

    # dates = construct_date_list(start_date, day_count)
    # dates = ['201801', '201802', '201803', '201804', '201805', '201806']
    dates = ['201806']

    output_path = '/Volumes/storage/cmsdata'
    local_parent = os.path.expanduser(os.path.join(output_path, hdfs_dataset))

    print("Local parent directory: {}".format(local_parent))

    # Create subdirectory for export
    os.makedirs(local_parent, exist_ok=True)

    for date in dates:
        print("Downloading data for date: {}".format(date))
        hdfs_path = '/'.join([hdfs_base_path, hdfs_dataset, date])
        local_path = os.path.join(local_parent, '{}.txt'.format(date))

        print("Paths: {} -> {}".format(hdfs_path, local_path))

        hdfs_fetch_command = 'hdfs dfs -cat {}/*'.format(hdfs_path)
        ssh_remote_command = ' ; '.join(ssh_setup_commands + [hdfs_fetch_command])

        # ssh_command = 'ssh lxplus7.cern.ch "source ~/analytix/CMSSpark/setup_lxplus.sh ; hdfs dfs -ls hdfs:///cms/users/mstemmer/jobmonitoring" > hdfs_download_test.txt'
        ssh_command = 'ssh {} "{}" > {}'.format(ssh_host, ssh_remote_command, local_path)
        print('SSH Command: "{}"'.format(ssh_command))

        call(ssh_command, shell=True)

        print("Completed date {}".format(date))


def construct_date_list(start=None, num=1):
    if not start:
        start = time.strftime("%Y%m%d", time.gmtime(time.time() - num * 60 * 60 * 24))

    elif len(start) != 8:
        raise Exception("Date is not in expected format!")

    startdatetime = datetime.datetime.strptime(start, '%Y%m%d')

    datelist = [startdatetime + datetime.timedelta(days=i) for i in range(0, num)]
    return [date.strftime('%Y%m%d') for date in datelist]


if __name__ == '__main__':
    main()
