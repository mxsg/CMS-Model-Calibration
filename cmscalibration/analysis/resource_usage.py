import pandas as pd

from data.dataset import Metric

_timestamp_col = 'time'
_slot_delta_col = 'deltaCores'
_total_slot_col = 'totalSlots'
_duration_col = 'duration'


def mean_jobslot_usage(jobs, start_time, end_time,
                       start_ts_col='StartedRunningTimeStamp',
                       end_ts_col='FinishedTimeStamp',
                       slot_col='NCores'):
    """Compute the total mean number of cores in use by the supplied job data frame. Returns a single scalar value."""

    df = calculate_jobslot_usage(jobs, start_time, end_time, start_ts_col, end_ts_col, slot_col)

    total_length = (df.index.max() - df.index.min()).total_seconds()

    # df = df.copy()
    # df['temp_multiplied'] = df[_duration_col] * df[_total_slot_col]
    # return df['temp_multiplied']
    #
    # return df[_duration_col].dot(df[_total_slot_col]) / total_length

    # For now, aggregate by resampling the data
    # Todo Is this accurate?
    df_upsampled = df.resample('s').pad()
    return df_upsampled.mean()


def calculate_jobslot_usage(jobs, start_time=None, end_time=None,
                            start_ts_col='StartedRunningTimeStamp',
                            end_ts_col='FinishedTimeStamp',
                            slot_col='NCores'):
    """Create and return a time series indicating the number of jobslots/cores in use by the supplied jobs data
    frame at any time.

    This is achieved by constructing a data frame containing the deltas in core usage a single job produces when it
    starts and stops.
    """

    # Subset to limit to the time frame given in parameters

    # Choose all jobs that end after the start time and start after the end time and hence have an effect
    # on the job slot number in use in the considered time frame.
    jobs = jobs[(jobs[end_ts_col] >= start_time) & (jobs[start_ts_col] <= end_time)]

    # Filter data to only include those that are non-null in both timestamps
    jobs = jobs[(jobs[start_ts_col].notnull()) & (jobs[end_ts_col].notnull())]

    ending_times = jobs[[end_ts_col, slot_col]]
    starting_times = jobs[[start_ts_col, slot_col]]

    starting_times = starting_times.rename(columns={start_ts_col: _timestamp_col}).copy()
    ending_times = ending_times.rename(columns={end_ts_col: _timestamp_col}).copy()

    # Ending jobs have a negative effect on the used job slots
    ending_times[slot_col] = -ending_times[slot_col]

    df = pd.concat([starting_times, ending_times])

    df = df.sort_values(_timestamp_col).set_index(_timestamp_col)
    df = df.rename(columns={slot_col: _slot_delta_col})

    # Aggregate values with the same timestamp
    df = df.groupby(df.index).sum()

    df[_total_slot_col] = df[_slot_delta_col].cumsum()

    # Compute differences between adjacent rows in seconds
    # Diff computes x[i] - x[i-1], shift afterwards to make sure the duration is associated with its start time stamp
    df[_duration_col] = df.index.to_series().diff().dt.total_seconds().shift(-1)

    # Cut off ramp up and down from before and after start and end times
    df = df.loc[(df.index >= start_time) & (df.index <= end_time)]

    return df