#!/usr/bin/env python
#-*- coding: utf-8 -*-

from subprocess import call
import sys

import ConfigParser
import time
import datetime


def constructDateList(start=None, num=1):
    if not start:
        start = time.strftime("%Y%m%d", time.gmtime(time.time() - num*60*60*24))

    elif len(start) != 8:
        raise Exception("Date is not in expected format!")

    startdatetime = datetime.datetime.strptime(start, '%Y%m%d')

    datelist = [startdatetime + datetime.timedelta(days=i) for i in range(0, num)]
    return [date.strftime('%Y%m%d') for date in datelist]

def main():
    "Main Function"

    ### General Setup

    # Configuration
    # Todo Replace this with a real configuration file
    config = ConfigParser.ConfigParser()
    config.add_section('analytix_collection')
    config.set('analytix_collection', 'start_date', '20180101')
    config.set('analytix_collection', 'day_count', '120')
    config.set('analytix_collection', 'hdfs_output_root', 'hdfs:///cms/users/mstemmer')
    config.set('analytix_collection', 'hdfs_output_directory', 'wmarchive')
    config.set('analytix_collection', 'setup_script_path', '/afs/cern.ch/user/m/mstemmer/analytix/setup.sh')
    config.set('analytix_collection', 'cmsspark_script', 'gridka_wmarchive.py')

    # Way of setting up configuration for Python 3
    # config['analytix_collection'] = { 'start_date': '20180101',
    #                                   'day_count': '31' }

    # Source is not an executable command
    # call(['source', config.get('analytix_collection', 'setup_script_path')])

    # Todo Is this needed?
    # call(['kinit'])

    ### Collection of Job Monitoring data

    dates = constructDateList(config.get('analytix_collection', 'start_date'), config.getint('analytix_collection', 'day_count'))
    print("=== Starting with collection from date: {}".format(dates[0]))

    print(dates)

    for date in dates:
        fout_path = '/'.join([config.get('analytix_collection', 'hdfs_output_root'), config.get('analytix_collection', 'hdfs_output_directory'), date])

        # Todo Using the shell functionality is possibly unsafe, but required for CMSSpark to successfully complete
        exitcode = call('run_spark' + ' ' + config.get('analytix_collection', 'cmsspark_script') + ' --fout=' + fout_path + ' --date=' + date + ' --yarn', shell=True)

        if exitcode != 0:
            print("=== Error while running Spark job for date {}, process exited with code: {}".format(date, exitcode))
            sys.exit(exitcode)



if __name__ == '__main__':
    main()
