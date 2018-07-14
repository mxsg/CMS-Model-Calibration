import json
import logging


def export_to_json_file(job_type_demands, path):
    logging.info("Exporting node types to file: {}".format(path))
    required_metrics = ['typeName',
                        'cpuDemandStoEx',
                        'ioTimeStoEx',
                        'ioTimeRatioStoEx',
                        'requiredJobslotsStoEx',
                        'relativeFrequency']

    for item in job_type_demands:
        # Check if all required metrics are present in dictionaries
        if not all(metric in item for metric in required_metrics):
            raise ValueError(
                "Cannot export demands, not all columns present. Required: {}, Encountered: {}".format(required_metrics,
                                                                                                       job_type_demands.keys()))

    with open(path, 'w') as outfile:
        json.dump(job_type_demands, outfile, indent=4, sort_keys=True)

    logging.info("Finished exporting job types.")
