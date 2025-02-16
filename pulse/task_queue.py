import abc
from abc import ABC
from concurrent.futures import Future, as_completed
from typing import Iterable

from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing

from pulse.models import Task, TaskResultStatus, TaskResult
from pulse.runtime import TaskExecutionError


class TaskQueue(ABC):
    # TODO remove
    @abc.abstractmethod
    def size(self) -> int:
        pass

    @abc.abstractmethod
    def send(self, task: Task) -> None:
        pass

    @abc.abstractmethod
    def receive(self, timeout: float) -> Iterable[TaskResult]:
        pass


class InMemoryTaskQueue(TaskQueue, LoggingMixing):
    def __init__(self, executor: TaskExecutor):
        super().__init__()
        self._executor = executor
        self._futures: list[Future] = []

    def size(self) -> int:
        return len(self._futures)

    def send(self, task: Task) -> None:
        future = self._executor.submit(task)
        self._futures.append(future)

    def receive(self, timeout: float) -> Iterable[TaskResult]:
        _completed = set()
        try:
            for future in as_completed(self._futures, timeout):
                try:
                    task = future.result()
                    yield TaskResult(id=task.id, status=TaskResultStatus.SUCCESS)
                except TaskExecutionError as e:
                    self.logger.exception(f"Task failed for id={e.task_id}")
                    yield TaskResult(id=e.task_id, status=TaskResultStatus.FAILED)
                finally:
                    _completed.add(future)
        except TimeoutError:
            num_jobs = len(self._futures)
            self.logger.warning("Timeout exceeded: %d jobs remaining.", num_jobs)
        finally:
            self._futures = [fut for fut in self._futures if fut not in _completed]
