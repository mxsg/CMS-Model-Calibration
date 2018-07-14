""" Utilities for date calculations. """

import datetime
import time


def construct_date_list(start=None, num=1):
    """ Construct a list of string dates in the format YYYYMMDD from a start date and a number of days. """
    if not start:
        start = time.strftime("%Y%m%d", time.gmtime(time.time() - num * 60 * 60 * 24))

    elif len(start) != 8:
        raise Exception("Date is not in expected format!")

    startdatetime = datetime.datetime.strptime(start, '%Y%m%d')

    datelist = [startdatetime + datetime.timedelta(days=i) for i in range(0, num)]
    return [date.strftime('%Y%m%d') for date in datelist]
