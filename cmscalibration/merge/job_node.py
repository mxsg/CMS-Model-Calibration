import logging

import pandas as pd


def match_jobs_to_node(jobs: pd.DataFrame, nodes: pd.DataFrame):
    all_job_nodes = jobs.HostName.unique()
    available_hosts = nodes.hostname.unique()

    logging.debug("Number of hosts jobs were run on: {}".format(len(all_job_nodes)))
    logging.debug("Number of hosts in resource environment: {}".format(len(available_hosts)))

    unmatched_job_nodes = [node for node in all_job_nodes if node not in available_hosts]

    nodes_without_jobs = [node for node in available_hosts if node not in all_job_nodes]

    logging.debug("Found {} nodes with jobs.".format(len([node for node in all_job_nodes if node in available_hosts])))
    logging.debug("Found {} unmatched job nodes: {}".format(len(unmatched_job_nodes), unmatched_job_nodes))

    logging.debug("Number of jobs per unmatched job node:")
    # Retrieve number of jobs that cannot be matched
    logging.debug(", ".join(["node: {}, jobs: {}".format(node, jobs[jobs['HostName'] == node].shape[0]) for node in unmatched_job_nodes]))

    logging.debug("Found {} nodes without jobs.".format(len(nodes_without_jobs)))

    # Merge jobs and nodes tables
    jobs_nodes = pd.merge(jobs, nodes, how='left', left_on='HostName', right_on='hostname')

    logging.debug("Joined {} job rows to {} node rows, result with {} rows"
                  .format(jobs.shape[0], nodes.shape[0], jobs_nodes.shape[0]))

    # logging.debug("Jobs joined with nodes:\n" + str(jobs_nodes))

    logging.debug("Jobs, nodes dtypes:\n{}".format(jobs_nodes.dtypes))
    return jobs_nodes