from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor

from pulse.logutils import LoggingMixing
from pulse.models import Job
from pulse.runtime import RuntimeManager


_runtime_manager: RuntimeManager


def initializer() -> None:
    global _runtime_manager
    _runtime_manager = RuntimeManager()


def _execute(job: Job) -> Job:
    runtime = _runtime_manager.get_runtime(job.runtime)
    runtime.run(job)
    return job


class JobExecutor(ABC):
    @abstractmethod
    def submit(self, job: Job) -> Future:
        pass


class ThreadJobExecutor(JobExecutor, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self._backend = ThreadPoolExecutor(initializer=initializer)

    def submit(self, job: Job) -> Future:
        return self._backend.submit(_execute, job)


class ProcessJobExecutor(JobExecutor, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self._backend = ProcessPoolExecutor(initializer=initializer)

    def submit(self, job: Job) -> Future:
        return self._backend.submit(_execute, job)
