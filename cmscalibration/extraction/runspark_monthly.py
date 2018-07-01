import os
from subprocess import call
import time
import datetime
import calendar

def main():
    ssh_setup_commands = ['source analytix/setup.sh']
    ssh_host = 'lxplus7.cern.ch'
    ssh_analytix_host = 'analytix'

    hdfs_base_path = 'hdfs:///cms/users/mstemmer'
    hdfs_dataset = 'jobmonitoring-monthly-remote-test'

    cmsspark_script = 'gridka_jm.py'

    start_year = 2018
    start_month = 1
    num_months = 1

    for i in range(num_months):
        year = start_year + (start_month + i - 1) // 12
        month = (start_month + i - 1) % 12 + 1

        year_month = datetime.date(year, month, 1).strftime('%Y%m')

        dates = list(map(lambda x: x.strftime('%Y%m%d'), dates_for_month(year, month)))

        hdfs_target = '/'.join([hdfs_base_path, hdfs_dataset, year_month])
        spark_command = 'run_spark {} --fout={} --date={} --yarn'.format(cmsspark_script, hdfs_target, ','.join(dates))
        print("Spark command: {}".format(spark_command))

        analytix_command = ' ; '.join(ssh_setup_commands + [spark_command])

        ssh_inner_command = 'ssh {} "{}"'.format(ssh_analytix_host, analytix_command)
        ssh_command ='ssh {} \'{}\''.format(ssh_host, ssh_inner_command)
        print("Full command: {}".format(ssh_command))

        call(ssh_command, shell=True)


def dates_for_month(year, month):
    # Returns a tuple with the weekday of the first day and the number of days
    num_days = calendar.monthrange(year, month)[1]
    return [datetime.date(year, month, day) for day in range(1, num_days+1)]


# def construct_date_list(start, num=1):
#     # if not start:
#     #     start = time.strftime("%Y%m%d", time.gmtime(time.time() - num * 60 * 60 * 24))
#
#     if len(start) != 6:
#         pass
#
#
#     if len(start) != 8:
#         raise Exception("Date is not in expected format!")
#
#     startdatetime = datetime.datetime.strptime(start, '%Y%m%d')
#
#     datelist = [startdatetime + datetime.timedelta(days=i) for i in range(0, num)]
#     return [date.strftime('%Y%m%d') for date in datelist]


if __name__ == '__main__':
    main()