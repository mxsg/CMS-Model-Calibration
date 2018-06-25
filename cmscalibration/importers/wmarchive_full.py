import json
import ast
import logging


class FullWMArchiveImporter:

    def import_from_file(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        data = []

        with open('./data/wmarchive-full-20180401.txt') as f:
            for line in f:
                record = ast.literal_eval(line)
                data.append(self.extract_data(record))

    def extract_data(record):
        metadata = record.get('meta_data', {})

        # Include general information and relevant data from FWJR metadata section

        result = {'task': record.get('task', ''),
                  'campaign': record.get('Campaign', '')}
        #               'submission_host': metadata.get('host', ''),
        #               'jobstate': metadata.get('jobstate', ''),
        #               'fwjr_timestamp': metadata.get('ts'),
        #               'fwjr_jobtype': metadata.get('jobtype')}

        for key, value in metadata.items():
            result[key] = value

        result['LFNArray'] = record.get('LFNArray')

        # Add performance data for first step that is cmsrun
        # Adapted from https://github.com/nilsleiffischer/WMArchive/blob/master/src/python/WMArchive/PySpark/RecordAggregator.py

        site = None
        acquisitionEra = None
        exitCode = None
        exitStep = None
        performance = None

        # Todo Aggregate over the real performance data here!
        for step in record['steps']:
            # Always use first non-null step for
            if site is None:
                site = step.get('site');

            if acquisitionEra is None:
                for output in step['output']:
                    acquisitionEra = output.get('acquisitionEra')
                    if acquisitionEra is not None:
                        break

            # Check for first non-zero exit code
            if not exitCode:
                for error in step['errors']:
                    exitCode = str(error.get('exitCode'))
                    exitStep = step.get('name')
                    if exitCode:
                        break

            if step['name'].startswith('cmsRun') and performance is None:
                performance = step['performance']

        result['site'] = site
        result['acquisitonEra'] = acquisitionEra
        result['exitCode'] = exitCode
        result['exitStep'] = exitStep
        result['performance'] = performance

        return result
