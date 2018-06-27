import ast
import logging

import pandas as pd


class FullWMArchiveImporter:

    def import_from_file(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        data = []

        with open('./data/wmarchive-full-20180401.txt') as f:
            for line in f:
                record = ast.literal_eval(line)
                data.append(self.extract_data(record))

        return pd.DataFrame(data)

    def extract_data(self, record):
        metadata = record.get('meta_data', {})

        # Include general information and relevant data from FWJR metadata section

        result = {'task': record.get('task', ''),
                  'campaign': record.get('Campaign', ''),
                  'PrepID': record.get('PrepId', ''),
                  'wmaid': record.get('wmaid', ''),
                  'dtype': record.get('dtype', ''),
                  'wmats': record.get('wmats'),
                  }
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

        cmsrun_start_time = None
        first_start_time = None

        steps = []
        step_start_times = []
        step_stop_times = []

        # Include detailed step information
        for step in record['steps']:

            # included_keys = ['start', 'stop', 'site']
            included_keys = ['site']

            step_info = {key: step.get(key) for key in included_keys}

            # TODO Add exit code information
            # step_info['exitCodes'] = [error.get('exitCode') for error in step.get('errors', [])]

            step_info['performance'] = step.get('performance', None)

            if 'start' in step and step.get('start') is not None:
                step_start_times.append(step.get('start'))
                step_info['start'] = step.get('start')
            if 'stop' in step and step.get('stop') is not None:
                step_stop_times.append(step.get('stop'))
                step_info['stop'] = step.get('stop')

            steps.append(step_info)

        result['steps'] = steps

        result['startTime'] = min(step_start_times) if step_start_times else None
        result['stopTime'] = max(step_stop_times) if step_stop_times else None

        # Todo Aggregate over the real performance data here!
        for step in record['steps']:
            if first_start_time is None:
                first_start_time = step.get('start')
            else:
                if step.get('start') < first_start_time:
                    first_start_time = step.get('start')

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

            if step['name'].startswith('cmsRun'):
                if performance is None:
                    performance = step['performance']

                step_start = step.get('start')

                if cmsrun_start_time is None or step_start < cmsrun_start_time:
                    cmsrun_start_time = step_start

        result['site'] = site
        result['acquisitonEra'] = acquisitionEra
        result['exitCode'] = exitCode
        result['exitStep'] = exitStep
        result['performance'] = performance
        result['cmsrunStartTime'] = cmsrun_start_time

        return result
