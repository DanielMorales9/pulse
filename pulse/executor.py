import os
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor

from pulse.constants import JobExecutorType
from pulse.logutils import LoggingMixing
from pulse.models import Task
from pulse.runtime import RuntimeManager


_runtime_manager: RuntimeManager


def initializer() -> None:
    global _runtime_manager
    _runtime_manager = RuntimeManager()


def _execute(task: Task) -> Task:
    runtime = _runtime_manager.get_runtime(task.runtime)
    runtime.run(task)
    return task


class TaskExecutor(ABC):
    @abstractmethod
    def submit(self, task: Task) -> Future:
        pass


class ThreadTaskExecutor(TaskExecutor, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self._backend = ThreadPoolExecutor(initializer=initializer)

    def submit(self, task: Task) -> Future:
        return self._backend.submit(_execute, task)


class ProcessTaskExecutor(TaskExecutor, LoggingMixing):
    def __init__(self) -> None:
        super().__init__()
        self._backend = ProcessPoolExecutor(initializer=initializer)

    def submit(self, task: Task) -> Future:
        return self._backend.submit(_execute, task)


class JobExecutorManager:
    EXECUTOR_CLASSES = {
        JobExecutorType.THREAD: ThreadTaskExecutor,
        JobExecutorType.PROCESS: ProcessTaskExecutor,
    }

    def get_executor(self, executor_type: JobExecutorType) -> TaskExecutor:
        return self.EXECUTOR_CLASSES[executor_type]()

    @staticmethod
    def parallelism(executor_type: JobExecutorType | str) -> int:
        base_parallelism = os.cpu_count() or 1
        if executor_type == JobExecutorType.THREAD:
            return base_parallelism + 4
        return base_parallelism
