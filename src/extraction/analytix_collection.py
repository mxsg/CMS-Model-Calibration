#!/usr/bin/env python
#-*- coding: utf-8 -*-

from subprocess import call
import ConfigParser
import time
import datetime

import pandas as pd

def constructDateList(start=None, num=1):
    if not start:
        start = time strftime("%Y%m%d", time.gmtime(time.time() - num*60*60*24))

    elif len(start) != 8:
        raise Exception("Date is not in expected format!")

    startdatetime = pd.to_datetime(start, format='%Y%m%d')
    datelist = pd.date_range(startdatetime, periods=num).strftime('%Y%m%d')

    return datelist

def main():
    "Main Function"

    ### General Setup

    # Configuration
    # Todo Replace this with a real configuration file
    config = ConfigParser.ConfigParser()
    config.add_section('analytix_collection')
    config.set('analytix_collection', 'start_date', '20180101')
    config.set('analytix_collection', 'end_date', '20180131')
    config.set('analytix_collection', 'day_count', '31')
    config.set('analytix_collection', 'hdfs_output_root', 'hdfs:///cms/users/mstemmer/')
    config.set('analytix_collection', 'setup_script_path', '/afs/cern.ch/user/m/mstemmer/analytix/setup.sh')

    # Way of setting up configuration for Python 3
    # config['analytix_collection'] = { 'start_date': '20180101',
    #                                   'day_count': '31' }

    # call(['source', config.get('analytix_collection', 'setup_script_path']))

    # Todo Is this needed?
    # call(['kinit'])

    ### Collection of Job Monitoring data

    dates = constructDateList(start_date, day_count)
    print(dates)



if __name__ == '__main__':
    main()
