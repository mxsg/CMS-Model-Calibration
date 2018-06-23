import logging

import pandas as pd
import numpy as np


def addPerformanceData(df):
    df['HSScorePerCore'] = df['hs06'] / df['cores']
    df['HSScorePerJobslot'] = df['hs06'] / df['jobslots']


def extractNodeTypes(df):
    # columns that the data have to be grouped by to retrieve the different node types
    grouped_cols = ['cpu model',
                    'jobslots',
                    'hs06',
                    'cores',
                    'interconnect',
                    'HSScorePerCore',
                    'HSScorePerJobslot']

    nodeTypes = df[grouped_cols + ['hostname']].groupby(grouped_cols, as_index=False)

    node_summary = nodeTypes.count()
    node_summary.rename(columns={'hostname': 'nodeCount',
                                 'cpu model': 'name',
                                 'HSScorePerCore': 'computingRatePerCore',
                                 'HSScorePerJobslot': 'computingRate',
                                 'jobslots': 'jobslots',
                                 'cores': 'cores'},
                        inplace=True)

    logging.debug("Node type summary:\n" + node_summary.to_string())

    return node_summary


# TODO Change this to compute the best possible combination of machines to get as close to
# the desired score as possible
# TODO Also compute other delta values
def scaleSiteWithNodeTypes(df, share, method='score'):
    if share < 0.0 or share > 1.0:
        raise ValueError('Share must be a value between 0.0 and 1.0.')

    if not method == 'score':
        raise ValueError('unknown scaling method')

    logging.debug("Scaling site")

    # Create a local copy of the input data frame
    node_df = df.copy()
    node_df['totalScore'] = node_df['nodeCount'] * node_df['hs06']

    total_site_score = node_df['totalScore'].sum()

    desired_score = share * total_site_score

    logging.debug("total site score: {}".format(total_site_score))
    logging.debug("Desired site score: {}".format(desired_score))

    node_df['fractionalCount'] = node_df['nodeCount'] * share

    node_df['currentCount'] = np.floor(node_df['fractionalCount']).astype(int)

    node_df['deltaScore'] = node_df['hs06'] * (node_df['fractionalCount'] - node_df['currentCount'])

    total_score_delta = node_df['deltaScore'].sum()
    logging.debug("Total delta score: {}".format(total_score_delta))

    sorted_nodes = node_df.sort_values(by=['deltaScore'], ascending=False)

    for index, row in sorted_nodes.iterrows():
        if total_score_delta <= 0.0:
            break

        node_df.loc[index, 'currentCount'] += 1
        total_score_delta -= node_df.loc[index, 'hs06']

    # Recompute delta scores
    node_df['deltaScore'] = node_df['hs06'] * (node_df['fractionalCount'] - node_df['currentCount'])

    current_scaled_score = node_df['hs06'].dot(node_df['currentCount'])

    logging.debug("Total score after scaling: {} (desired {}), score delta: {} ({} relative error)"
                  .format(current_scaled_score, desired_score, -total_score_delta, - total_score_delta / desired_score))

    result = df.copy()
    result['nodeCount'] = node_df['currentCount']
    result = result[result['nodeCount'] > 0]

    # Reindex resulting data frame
    result.index = range(len(result.index))

    logging.debug("Scaled node environment:\n" + result.to_string())

    return result
