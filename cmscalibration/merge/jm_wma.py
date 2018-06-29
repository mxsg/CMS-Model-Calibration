import logging

import pandas as pd


def match_jobs(jmdf, wmdf):
    jmdf, wmdf = prepare_matching(jmdf, wmdf)


def prepare_matching(jmdf, wmdf):
    logging.debug("Matching data frames, jmdf with {} entries, wma {}.".format(jmdf.shape[0], wmdf.shape[0]))
    jmdf = jmdf.copy()
    wmdf = wmdf.copy()

    # Subset data from JobMonitoring to only include jobs submitted by WMAgent
    jmdf = jmdf[jmdf['SubmissionTool'] == 'wmagent']

    preprocess_jm(jmdf)

    wmdf_key = 'wmaid'
    wmdf = wmdf.drop_duplicates(wmdf_key)
    wmdf.set_index(wmdf_key, inplace=True)
    wmdf[wmdf_key] = wmdf.index


    # Keep the last row, as it the one that ran the longest
    # jmdf = jmdf.drop_duplicates(jmdf_key, keep='last')

    return jmdf, wmdf

def match_on_files(jmdf, wmdf):
    jmdf_file_col = 'FileName'

    jmdf_key = 'JobId'
    jm_files = jmdf_file_df(jmdf, jmdf_file_col, jmdf_key)
    wm_files = wmdf_file_df(wmdf)



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


def wmdf_file_df(wmdf):
    # Files from WMArchive table
    s = wmdf.apply(lambda x: pd.Series(x['LFNArray']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'FileName'
    wm_files = wmdf.drop('LFNArray', axis=1).join(s)
    wm_files = wm_files[['FileName', 'wmaid']]
    return wm_files


def jmdf_file_df(jmdf, file_col, key):
    # Create data frame with all files from JM table
    key_in_cols = (key in jmdf.columns)
    jm_files = jmdf.reset_index(drop=key_in_cols)[[file_col, key]].copy()
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
