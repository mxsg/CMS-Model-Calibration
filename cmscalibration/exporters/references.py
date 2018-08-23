import logging
from typing import Dict

import pandas as pd

from data.dataset import Metric


def export_walltimes(partitions: Dict[str, pd.DataFrame], path: str, dropna=True):

    logging.debug("Exporting reference walltimes to {}".format(path))

    # Todo Check for required columns?
    # required_columns = [type_column, Metric.WALL_TIME.value]
    #
    # if not all(col in jobs.columns for col in required_columns):
    #     raise ValueError("Not all required columns are present in data frame!")

    dfs = []

    for partition_name, df in partitions.items():
        walltimes = df[[Metric.WALL_TIME.value]].copy()
        walltimes['type'] = partition_name
        dfs.append(walltimes)

    result = pd.concat(dfs)
    result.rename(columns={Metric.WALL_TIME.value: 'walltime'}, inplace=True)

    if dropna:
        result.dropna(inplace=True)

    result.to_csv(path, header=True)