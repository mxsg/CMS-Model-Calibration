import logging

import numpy as np


def add_performance_data(df):
    df['HSScorePerCore'] = df['hs06'] / df['cores']
    df['HSScorePerJobslot'] = df['hs06'] / df['jobslots']

    def logical_cores(physical_cores, jobslots):
        return 2 * physical_cores if jobslots > physical_cores else physical_cores

    df['coresLogical'] = df.apply(lambda x: logical_cores(x['cores'], x['jobslots']), axis='columns')


def extract_node_types(df):
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

    logging.debug("Total job slots in resource environment: {}".format(df['jobslots'].sum()))
    logging.debug("Total physical cores in resource environment: {}".format(df['cores'].sum()))
    logging.debug("Total logical cores in resource environment: {}".format(df['coresLogical'].sum()))

    logging.debug("Node type summary:\n" + node_summary.to_string())

    return node_summary


def add_logical_core_count(df):
    def logical_cores(physical_cores, jobslots):
        return 2 * physical_cores if jobslots > physical_cores else physical_cores

    df['coresLogical'] = df.apply(lambda x: logical_cores(x['cores'], x['jobslots']), axis='columns')


def scale_site_by_jobslots(df, count):
    """
    Scale a resource environment (data frame with node type information) to the supplied share. This method uses
    the number of jobslots in each node as a target metric.
    """

    jobslot_col = 'jobslots'
    count_col = 'nodeCount'

    # Compute the total number of slots in the dataframe and the
    total_slots = df['jobslots'].dot(df['nodeCount'])
    share = count / total_slots

    return scale_dataframe(df, share, count_col, jobslot_col)


def scale_site_by_benchmark(df, share, method='score'):
    """
    Scale a resource environment (data frame with node type information) to the supplied share.
    The 'score' method uses benchmarking scores to scale the environment.
    """

    if not method == 'score':
        raise ValueError('unknown scaling method')

    # TODO Replace these with generic values
    benchmark_score_col = 'hs06'
    node_count_col = 'nodeCount'

    return scale_dataframe(df, share, node_count_col, benchmark_score_col)


def scale_dataframe(dataframe, share, count_col, score_col):
    """
    Scale the dataframe: Create a new, modified data frame that contains values in its count_col column such that
    the sum of the metric_col column is as close as possible to the proportion provided as a parameter.

    :param dataframe: The data frame containing the original site configuration.
    :param score_col: The column name of the metric that should be used as a target metric for scaling. This is being
    summed for all rows in the data frame.
    :param count_col: The column name of the column containing a count of the entities.
    :param share: The target share of the sum of the metric column the data frame should be scaled to.
    :return: A copied dataframe where the count_col entries are modified in such a way that the total sum of the metric
    column is as close as possible to the share provided as a parameter.
    """

    total_score = 'temp_totalScore'
    fractional_count = 'temp_fractional_count'
    current_count = 'temp_current_count'
    delta_score = 'temp_delta_score'

    if share < 0.0 or share > 1.0:
        raise ValueError('Share must be a value between 0.0 and 1.0.')

    logging.debug("Scaling dataframe with metric {}, count column {} to share {}".format(score_col, count_col, share))

    # Create a local copy of the input data frame
    df = dataframe.copy()

    # The total contribution to the metric of a single row in the data frame
    df[total_score] = df[count_col] * df[score_col]

    total_frame_score = df[total_score].sum()
    desired_score = share * total_frame_score

    logging.debug("Total score: {}".format(total_frame_score))
    logging.debug("Desired score: {}".format(desired_score))

    # Compute theoretical optimum scaled counts
    df[fractional_count] = df[count_col] * share

    # The rounded down counts will be used as a starting point for optimization of the metric to
    # be as close as possible to the target metric
    df[current_count] = np.floor(df[fractional_count]).astype(int)

    # Compute the delta between the optimal and current value
    # Negative: the score contribution is too low
    # Positive: the score contribution is too high
    df[delta_score] = df[score_col] * (df[fractional_count] - df[current_count])

    total_score_delta = df[delta_score].sum()
    logging.debug("Total score delta: {}".format(total_score_delta))

    # TODO Future work: Optimize this to find an optimal solution, e.g. with dynamic programming

    # Greedily increase the count for a row at a time until the score target is reached
    sorted_nodes = df.sort_values(by=[delta_score], ascending=False)

    for index, row in sorted_nodes.iterrows():
        if total_score_delta <= 0.0:
            # The target has been reached
            break

        df.loc[index, current_count] += 1
        total_score_delta -= df.loc[index, score_col]

    # Recompute delta scores and total scaled score
    df[delta_score] = df[score_col] * (df[fractional_count] - df[current_count])
    current_scaled_score = df[score_col].dot(df[current_count])

    logging.debug("Total score after scaling: {} (desired {}), score delta: {} ({} relative error)"
                  .format(current_scaled_score, desired_score, -total_score_delta, - total_score_delta / desired_score))

    result = df.copy()
    result[count_col] = df[current_count]

    # Only include rows with positive count
    result = result[result[count_col] > 0]

    # Reindex resulting data frame
    result.index = range(len(result.index))

    # Drop temporary columns
    result = result[dataframe.columns]

    logging.debug("Scaled dataframe:\n" + result.to_string())

    return result
