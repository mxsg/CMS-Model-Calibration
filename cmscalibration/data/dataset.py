from enum import Enum, auto


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
