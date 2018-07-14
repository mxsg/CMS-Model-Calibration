from enum import Enum

class Metric(Enum):
    """ This contains the possible metrics a dataset can exhibit.

    Available metrics currently include ones from the following categories:
    - performance metrics
    - timing information and time stamps
    - entity (e.g., job or node) category information
    """

    # Performance data
    CPU_TIME = 'CPUTime'
    WALL_TIME = 'WallTime'
    USED_CORES = 'UsedCores'

    EVENT_COUNT = 'EventCount'
    INPUT_EVENT_COUNT = 'InputEventCount'
    OUTPUT_EVENT_COUNT = 'OutputEventCount'

    IO_TIME = 'IOTime'
    READ_TIME = 'IOReadTime'
    WRITE_TIME = 'IOWriteTime'

    AVERAGE_READ_SPEED = 'AvgReadSpeed'
    AVERAGE_WRITE_SPEED = 'AvgWriteSpeed'

    TOTAL_READ_MB = 'TotalReadMB'
    TOTAL_WRITE_MB = 'TotalWriteMB'

    CPU_EFFICIENCY = 'CPUEfficiency'

    # Time stamps
    START_TIME = 'StartTime'
    STOP_TIME = 'StopTime'
    FINISHED_TIME = 'FinishedTime'
    TIMESTAMP = 'TimeStamp'

    EXIT_CODE = 'ExitCode'
    HOST_NAME = 'HostName'

    # Category information
    WORKFLOW = 'Workflow'
    SUBMISSION_TOOL = 'SubmissionTool'
    JOB_TYPE = 'JobType'
    JOB_CATEGORY = 'JobCategory'


# TODO Remove this!
class Metrics:
    # ID = auto()

    CPU_TIME = 'cpu_time'
    WALL_TIME = 'wall_time'
    CORE_COUNT = 'core_count'

    START_TIME = 'start_time'
    STOP_TIME = 'stop_time'
    FINISHED_TIME = 'finished_time'

    WORKFLOW = 'workflow'
    SUBMISSION_TOOL = 'submission_tool'


class Dataset:
    def __init__(self, df, name='dataset', start=None, end=None, sep='#', extra_dfs=dict()):
        self.df = df
        self.name = name
        self.start = start
        self.end = end
        self.sep = sep
        self.extra_dfs = extra_dfs

    @property
    def sections(self):
        if not self.df:
            return []

        # Retrieve a list of all column sections (all string parts after the initial separator)
        column_sections = [col.split(self.sep)[1] for col in self.df.columns if self.sep in col]

        sections = set(column_sections)
        return sorted(list(sections))

    @property
    def metrics(self):
        if not self.df:
            return []

        column_metric_strings = [col.split(self.sep)[0] for col in self.df.columns]

        metrics = set()
        for colstring in column_metric_strings:
            try:
                metrics.add(Metric(colstring))
            except ValueError:
                continue

        return sorted(list(set(metrics)))

    def cols_for_section(self, section=''):
        # Filter all column names that contain the section name
        if not section:
            filtered_colnames = [colname for colname in self.df.columns if not colname.contains(self.sep)]
        else:
            filtered_colnames = [colname for colname in self.df.columns if colname.contains(self.sep + section)]

        return filtered_colnames

    def col(self, metric, section=None):
        if not section:
            colname = metric.value
        else:
            colname = self.sep.join(metric.value, section)

        if colname not in self.df.columns:
            raise ValueError(f'Metric {metric} is not contained in the dataset "{self.name}!"')
        else:
            return metric.value

# TODO Remove this.
class JobsAndFilesDataSet:

    def __init__(self, jobs, files):
        self._jobs = jobs
        self._files = files

    @property
    def jobs(self):
        return self._jobs.copy()

    @property
    def files(self):
        return self._files.copy()

# TODO Remove this.
class JobsDataset:
    def __init__(self, jobs, metric_dict):
        self._jobs = jobs

        for metric, colname in metric_dict.items():
            if colname not in jobs.columns:
                raise ValueError(
                    "Column name {}, associated to metric {} not found in columns.".format(colname, metric.name))

        self._metrics = metric_dict.copy()

    @property
    def metrics(self):
        return self._metrics

    @property
    def jobs(self):
        return self._jobs.copy()

    @jobs.setter
    def jobs(self, jobs):
        # Check whether all columns that are referenced by metrics exist
        if not all((value for _, value in self.metrics.items()) in jobs.columns):
            raise ValueError("Cannot set new jobs frame, not all metrics contained in columns!")

        self._jobs = jobs

    def add_metric(self, metric: Metrics, metric_col: str):
        if metric_col not in self.jobs.columns:
            raise ValueError("Cannot set metric {}, column {} not found.".format(metric.name, metric_col))

        if metric in self._metrics:
            raise ValueError("Cannot set metric {}, already contained in metrics.".format(metric.name))

        self._metrics.update(metric, metric_col)

    def col(self, metric):
        if metric not in self.metrics:
            raise ValueError("Metric {} is not contained in metrics.".format(metric.name))

        return self._metrics.get(metric)
