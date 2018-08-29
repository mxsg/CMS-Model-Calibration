from abc import ABCMeta, abstractmethod


class CalibrationWorkflow(metaclass=ABCMeta):
    """Interface a calibration workflow has to implement to be executed."""

    @abstractmethod
    def run(self):
        return NotImplemented
