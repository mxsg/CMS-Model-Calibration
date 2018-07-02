import logging

import pandas as pd


# class JobReportMatcher:
#     def __init__(self, timestamp_tolerance=0):
#         self._timestamp_tolerance = timestamp_tolerance
#
#     def match(self, jm_dataset, wm_dataset):


def match_on_cputime(jm_dataset, wm_dataset):
    jmdf = jm_dataset.jobs
    wmdf = wm_dataset.jobs


def match_jobs(jmdf, wmdf):
    # split_keys = ['TaskMonitorId', 'WNHostName']
    split_key = 'TaskMonitorId'

    timestamp_col = 'JobExecExitTimeStamp'
    # TODO Change this to use configuration
    dates = pd.date_range(jmdf['JobExecExitTimeStamp'].min().normalize(),
                          jmdf['JobExecExitTimeStamp'].max().normalize(),
                          freq='D')

    jmdf_subsets = [subset_day(jmdf, timestamp_col, date) for date in dates]
    print("Daily counts: {}".format(list(map(len, jmdf_subsets))))

    wmdf_subsets = [subset_day(wmdf, 'stopTime', date) for date in dates]
    print("Daily counts: {}".format(list(map(len, wmdf_subsets))))

    # TODO Do not subset here
    jmdf = jmdf[jmdf['JobExecExitTimeStamp'].notnull()].copy()
    wmdf = wmdf[wmdf['stopTime'].notnull()].copy()

    jmdf['jmdfStopDay'] = jmdf['JobExecExitTimeStamp'].dt.normalize()
    jmdf['jmdfStopHour'] = jmdf['JobExecExitTimeStamp'].dt.hour
    wmdf['wmdfStopDay'] = wmdf['stopTime'].dt.normalize()
    wmdf['wmdfStopHour'] = wmdf['stopTime'].dt.hour

    print("Splitting into groups by day and TaskMonitorId")
    groups = split_with_keys(jmdf, ['jmdfStopDay', 'jmdfStopHour'])

    match_df_list = []
    print("Number of groups to match: {}".format(len(groups)))

    matched = 0
    to_match = jmdf.shape[0]

    for group_keys, group_jmdf in groups:
        print("Matching group with {} entries".format(len(group_jmdf)))
        # group_wmdf = wmdf[(wmdf['wmdfStopDay'] == group_keys[0]) & (wmdf['wmdfStopDay'] == group_keys[0]) & (wmdf['TaskMonitorId'] == group_keys[1])]
        group_wmdf = wmdf[(wmdf['wmdfStopDay'] == group_keys[0]) & (wmdf['wmdfStopDay'] == group_keys[0])]
        # group_wmdf = wmdf[wmdf['TaskMonitorId'] == group_key]
        #
        # print("matching group lengths: wmdf {}, jmdf {}".format(group_wmdf.shape[0], group_jmdf.shape[0]))
        matches = match_on_files(group_jmdf, group_wmdf)
        # print("{} matches found".format(matches.shape[0]))
        matched += group_jmdf.shape[0]
        print("Total matched {}, to be done {} ({} %)".format(matched, to_match, matched / to_match * 100))
        match_df_list.append(match_on_files(group_jmdf, group_wmdf))

    file_matches = pd.concat(match_df_list)

    return file_matches


def subset_day(df, column, timestamp):
    day = timestamp.normalize()
    next_day = day + pd.to_timedelta('1d')

    return df[(df[column] >= day) & (df[column] < next_day)]


def split_by_datetime(df, column, timestamps, freq='D'):
    sorted_timestamps = sorted(timestamps)
    first_datetime = sorted_timestamps[0]
    last_datetime = sorted_timestamps[-1]

    # dates = pd.date_range

    null_values = df[df[column].isnull()]
    before_first = df[df[column] < first_datetime]
    after_last = df[df[column] > last_datetime]


def prepare_matching(jmdf, wmdf):
    logging.debug("Matching data frames, jmdf with {} entries, wma {}.".format(jmdf.shape[0], wmdf.shape[0]))
    jmdf = jmdf.copy()
    wmdf = wmdf.copy()

    # Subset data from JobMonitoring to only include jobs submitted by WMAgent
    # TODO Add this again!
    # jmdf = jmdf[jmdf['SubmissionTool'] == 'wmagent']

    preprocess_jm(jmdf)

    wmdf_key = 'wmaid'
    wmdf = wmdf.drop_duplicates(wmdf_key)
    wmdf.set_index(wmdf_key, inplace=True)
    wmdf[wmdf_key] = wmdf.index

    wmdf['wmaWrapCPU'] = wmdf.apply(lambda x: x['performance'].get('cpu').get('TotalJobCPU') if x['performance'] is not None else None,
                                    axis=1)

    # Keep the last row, as it the one that ran the longest
    # jmdf = jmdf.drop_duplicates(jmdf_key, keep='last')
    return jmdf, wmdf

def match_on_files(jmdf, wmdf):
    jmdf_file_col = 'FileName'

    jmdf_key = 'JobId'
    jm_files = jmdf_file_df(jmdf, jmdf_file_col, jmdf_key,
                            additional_cols=['StartedRunningTimeStamp', 'JobExecExitTimeStamp'])
    wm_files = wmdf_file_df(wmdf, additional_cols=['startTime', 'stopTime'])

    file_matches = jm_files.merge(wm_files, on='FileName')

    # After merging, we do not need to retain the file names themselves, but only the association
    possible_matches = file_matches.drop(columns='FileName').drop_duplicates()

    # TODO Make this generic for general timestamps
    def filter_distinct_matches(group):
        filter_threshold = 20
        group = group[
            abs((group['startTime'] - group['StartedRunningTimeStamp']).dt.total_seconds()) < filter_threshold]
        group = group[abs((group['stopTime'] - group['JobExecExitTimeStamp']).dt.total_seconds()) < filter_threshold]
        return group

    grouped = possible_matches.groupby(jmdf_key)

    filtered_matches = grouped.apply(filter_distinct_matches)

    perfect_matches = filtered_matches.groupby('JobId').filter(lambda x: len(x) == 1)

    if perfect_matches.empty:
        return pd.DataFrame()
    else:
        return perfect_matches[['JobId', 'wmaid']]


def match_on_metadata(jmdf, wmdf):
    pass

    # for jm_row in jmdf.iterrows():
    #     start_matches = wmdf


def abs_time_diff(time1, time2):
    print("Diff diff between: {} and {} ".format(time1, time2))
    abs((time1 - time2).total_seconds())


def aggregate_jmdf(jmdf):
    # TODO For now, simply drop rows with duplicated JobId.
    # Later, generate unique identifier instead of doing this!
    # key_cols = ['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp']
    # jmdf_key = add_unique_id(jmdf, key_cols)

    jmdf_keys = ['JobId', 'StartedRunningTimeStamp', 'FinishedTimeStamp']

    jobs = jmdf.drop(columns='FileName').drop_duplicates(jmdf_keys)

    return jobs

    # jmdf_key = 'JobId'
    # jmdf = jmdf.set_index(jmdf_key)
    # # Duplicate key into its own column again
    # jmdf[jmdf_key] = jmdf.index


def check_task_id_diffs(jmdf, wmdf):
    jmdf_wmagent_counts = jmdf.TaskMonitorId.value_counts()
    jmdf_wmagent_counts.name = 'task_jm'

    wmdf_wmagent_counts = wmdf.TaskMonitorId.value_counts()
    wmdf_wmagent_counts.name = 'task_wm'

    wmdf_not_jmdf = set(wmdf_wmagent_counts.index) - set(jmdf_wmagent_counts.index)
    logging.debug("{} wmdf workflows not in jmdf: {}".format(len(wmdf_not_jmdf), wmdf_not_jmdf))

    jmdf_not_wmdf = set(jmdf_wmagent_counts.index) - set(wmdf_wmagent_counts.index)
    logging.debug("{} jmdf workflows not in wmdf: {}".format(len(jmdf_not_wmdf), jmdf_not_wmdf))

    both_counts = pd.concat([wmdf_wmagent_counts, jmdf_wmagent_counts], axis=1)
    more_jmdf = both_counts[both_counts.task_wm < both_counts.task_jm]

    logging.debug("Workflows more often in jmdf than wmdf: \n{}".format(more_jmdf))


def wmdf_file_df(wmdf, additional_cols=None):
    if additional_cols is None:
        additional_cols = []

    wmdf = wmdf[['wmaid', 'LFNArray'] + additional_cols]

    # Files from WMArchive table
    s = wmdf.apply(lambda x: pd.Series(x['LFNArray']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'FileName'
    wm_files = wmdf.drop('LFNArray', axis=1).join(s)

    wm_files = wm_files[['FileName', 'wmaid'] + additional_cols]
    return wm_files


def jmdf_file_df(jmdf, file_col, key, additional_cols=None):
    if additional_cols == None:
        additional_cols = []

    # Create data frame with all files from JM table
    key_in_cols = (key in jmdf.columns)
    jm_files = jmdf.reset_index(drop=key_in_cols)[[file_col, key] + additional_cols].copy()
    return jm_files


def preprocess_jm(jmdf):
    # Drop the first slash from the file name, if present
    jmdf['FileName'] = jmdf['FileName'].replace('^//', '/', regex=True)
    jmdf['TaskMonitorId'] = jmdf['TaskMonitorId'].replace('^wmagent_', '', regex=True)


def add_unique_id(jmdf, keys, id_name='uniqueId'):
    """
    Adds a unique identifier to the data frame. Note that this operation modifies the passed data frame. This either
    return a column from the supplied keys or creates a new column called id_name.

    :param jmdf: The data frame the unique identifier should be added to. This data frame is modified.
    :param keys: The columns to be used as keys for the data frame. The  unique identifier will be the same if and only
    if their combination is identical.
    :return: The name of the column chosen as identifier, either a value from keys or a new column
    """

    # This operation is potentially slow, so test whether one of the supplied keys is already adequate

    for key in keys:
        if jmdf.duplicated(key).sum() == 0:
            return key

    # No key has been found, create one from a combination of the keys.
    def make_identifier(df):
        str_id = df.apply(lambda x: '_'.join(map(str, x)), axis=1)
        return pd.factorize(str_id)[0]

    jmdf[id_name] = make_identifier(jmdf[keys])
    return id_name


def split(df, group):
    grouped = df.groupby(group)
    return [grouped.get_group(x) for x in grouped.groups]


def split_with_keys(df, group):
    grouped = df.groupby(group)
    return [(x, grouped.get_group(x)) for x in grouped.groups]


# def match_by_files(self, jmdf, wmdf):
#     # This requires the two data frames to be limited to the same time period
#     pass

def split_by_date(jmdf, wmdf):
    pass
