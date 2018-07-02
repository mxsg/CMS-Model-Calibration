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