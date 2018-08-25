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
    CPU_TIME_PER_CORE = 'CPUTimePerCore'
    WALL_TIME = 'WallTime'

    INIT_TIME = 'InitTime'

    USED_CORES = 'UsedCores'  # Number of jobslots a job needs
    USED_THREADS = 'UsedThreads'  # Number of Threads may also be lower
    EVENT_STREAM_COUNT = 'UsedEventStreams'

    EVENT_COUNT = 'EventCount'
    EVENT_COUNT_FROM_PERF = 'EventCountHeuristic'

    EVENT_THROUGHPUT = 'EventThroughput'

    INPUT_EVENT_COUNT = 'InputEventCount'
    OUTPUT_EVENT_COUNT = 'OutputEventCount'

    TOTAL_READ_DATA = 'TotalReadDataMiB'
    TOTAL_WRITTEN_DATA = 'TotalWriteDataMiB'

    AVERAGE_READ_SPEED = 'AvgReadSpeed'
    AVERAGE_WRITE_SPEED = 'AvgWriteSpeed'

    IO_TIME = 'IOTime'
    READ_TIME = 'IOReadTime'
    WRITE_TIME = 'IOWriteTime'

    CPU_EFFICIENCY = 'CPUEfficiency'
    OVERALL_CPU_EFFICIENCY = 'OverallJobCPUEfficiency'
    CPU_IDLE_TIME = 'CPUIdleTime'
    CPU_IDLE_TIME_RATIO = 'CPUIdleTimeRatio'

    CPU_IDLE_TIME_PER_EVENT = 'CPUIdleTimePerEvent'
    CPU_TIME_PER_EVENT = 'CPUTimePerEvent'
    CPU_DEMAND_PER_EVENT = 'CPUDemandPerEvent'

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
    TASK_NAME = 'TaskName'


class Dataset:
    """A dataset is a data frame with additional information associated with it. Besides the main data frame itself,
    it is named, has a time period the data is valid for and can contain additional data frames as associated info.

    A dataset can also contain sections of columns which belong together (e.g. originally come from the same dataset).
    """

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
        """Return the sections that are present in this dataset."""
        if not self.df:
            return []

        # Retrieve a list of all column sections (all string parts after the initial separator)
        column_sections = [col.split(self.sep)[1] for col in self.df.columns if self.sep in col]

        sections = set(column_sections)
        return sorted(list(sections))

    @property
    def metrics(self):
        """Return all metrics present in this dataset."""
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
        """Return the columns which are present in a specific section of the dataset."""
        # Filter all column names that contain the section name
        if not section:
            filtered_colnames = [colname for colname in self.df.columns if not colname.contains(self.sep)]
        else:
            filtered_colnames = [colname for colname in self.df.columns if colname.contains(self.sep + section)]

        return filtered_colnames

    def col(self, metric, section=None):
        """Return the column for the supplied metric from the dataframe."""
        if not section:
            colname = metric.value
        else:
            colname = self.sep.join(metric.value, section)

        if colname not in self.df.columns:
            raise ValueError(f'Metric {metric} is not contained in the dataset "{self.name}!"')
        else:
            return metric.value
