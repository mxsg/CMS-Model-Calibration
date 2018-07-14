from enum import Enum


class Metric(Enum):
    """ Contains the possible metrics a dataset can exhibit.

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


class Dataset:
    """A dataset is a data frame with additional information associated with it. Besides the main data frame itself,
    it is named, has a time period the data is valid for and can contain additional data frames as associated info."""

    def __init__(self, df, name='dataset', start=None, end=None, sep='#', extra_dfs=None):
        self.df = df
        self.name = name
        self.start = start
        self.end = end
        self.sep = sep

        if extra_dfs is None:
            extra_dfs = dict()

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
