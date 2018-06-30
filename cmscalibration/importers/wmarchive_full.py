import ast
import logging

import pandas as pd


class FullWMArchiveImporter:

    def import_from_file(self, path):
        logging.info("Reading WMArchive file from {}.".format(path))

        data = []

        with open(path) as f:
            for line in f:
                record = ast.literal_eval(line)
                data.append(self.extract_data(record))

        wmdf = pd.DataFrame(data).drop_duplicates('wmaid')
        self.convert_columns(wmdf)

        return wmdf

    def convert_columns(self, df):
        df['startTime'] = pd.to_datetime(df['startTime'], unit='s')
        df['stopTime'] = pd.to_datetime(df['stopTime'], unit='s')
        df['cmsrunStartTime'] = pd.to_datetime(df['cmsrunStartTime'], unit='s')
        df['wmats'] = pd.to_datetime(df['wmats'], unit='s')
        df['ts'] = pd.to_datetime(df['ts'], unit='s')
        df['TaskMonitorId'] = df['task'].str.split('/').apply(lambda x: x[1])

    def extract_data(self, record):

        # Include general information and relevant data from FWJR metadata section

        result = {
            'task': record.get('task'),
            'PrepID': record.get('PrepId'),
            'Campaign': record.get('Campaign'),
            'wmaid': record.get('wmaid'),
            'dtype': record.get('dtype'),
            'wmats': record.get('wmats'),
            'LFNArray': record.get('LFNArray')
        }

        metadata = record.get('meta_data', {})
        for key, value in metadata.items():
            result[key] = value

        # Add performance data for first step that is cmsrun
        # Adapted from https://github.com/nilsleiffischer/WMArchive/blob/master/src/python/WMArchive/PySpark/RecordAggregator.py

        site = None
        acquisitionEra = None
        exitCode = None
        exitStep = None
        performance = None

        cmsrun_input_events = None
        cmsrun_output_events = None

        steps = []
        step_start_times = []
        step_stop_times = []

        cmsrun_start_time = None
        cmsrun_stop_time = None

        # def get_event_counts(step_dict):
        #     input_list = step.get('input', [])
        #     step_input_events = sum([input_step.get('events', 0) for input_step in input_list])
        #
        #     output_list = step.get('output', [])
        #     step_output_events = sum([output_step.get('events', 0) for output_step in output_list])
        #
        #     return step_input_events, step_output_events

        # Include detailed step information
        for step in record['steps']:

            included_keys = ['start', 'stop', 'site', 'name']

            step_info = {key: step.get(key) for key in included_keys}

            # TODO Add exit code information
            step_info['exitCodes'] = [error.get('exitCode') for error in step.get('errors', [])]

            # step_info['performance'] = step.get('performance', None)

            if 'start' in step and step.get('start') is not None:
                step_start_times.append(step.get('start'))
                step_info['start'] = step.get('start')
            if 'stop' in step and step.get('stop') is not None:
                step_stop_times.append(step.get('stop'))
                step_info['stop'] = step.get('stop')

            input_list = step.get('input', [])
            step_input_events = sum(filter(None, [input_step.get('events', 0) for input_step in input_list]))
            step_info['inputEvents'] = step_input_events

            output_list = step.get('output', [])
            step_output_events = sum(filter(None, [output_step.get('events', 0) for output_step in output_list]))
            step_info['outputEvents'] = step_output_events

            steps.append(step_info)

        result['steps'] = steps

        result['startTime'] = min(step_start_times) if step_start_times else None
        result['stopTime'] = max(step_stop_times) if step_stop_times else None

        # Todo Aggregate over the real performance data here!
        for step in record['steps']:
            # Always use first non-null step for
            if site is None:
                site = step.get('site')

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
                step_stop = step.get('stop')

                if cmsrun_start_time is None or step_start < cmsrun_start_time:
                    cmsrun_start_time = step_start

                if cmsrun_stop_time is None or step_stop > cmsrun_stop_time:
                    cmsrun_stop_time = step_stop

                input_list = step.get('input', [])
                cmsrun_input_events = sum(filter(None, [input_step.get('events', 0) for input_step in input_list]))

                output_list = step.get('output', [])
                cmsrun_output_events = sum(filter(None, [output_step.get('events', 0) for output_step in output_list]))

        result['site'] = site
        result['acquisitonEra'] = acquisitionEra
        result['exitCode'] = exitCode
        result['exitStep'] = exitStep
        result['performance'] = performance
        result['cmsrunStartTime'] = cmsrun_start_time

        result['cmsrunInputEvents'] = cmsrun_input_events
        result['cmsrunOutputEvents'] = cmsrun_output_events

        return result
