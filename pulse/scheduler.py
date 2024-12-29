from dataclasses import field, dataclass
from concurrent.futures import as_completed, Future
from datetime import datetime
from typing import Self, Any

from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM, JobRunStatus, TaskInstanceStatus
from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing
from pulse.models import JobRun, TaskInstance
from pulse.repository import (
    JobRepository,
    JobRunRepository,
    TaskInstanceRepository,
)
from pulse.runtime import TaskExecutionError


class RuntimeInconsistencyCheckError(Exception):
    """Raised when an inconsistency is found during a runtime consistency check."""


def list_factory() -> list[str]:
    return []


@dataclass(frozen=True)
class TasksResults:
    success: list[str] = field(default_factory=list_factory)
    failed: list[str] = field(default_factory=list_factory)


def check_for_inconsistent_task_instances(result: TasksResults) -> None:
    """Checks for inconsistent task instances results, such as duplicate IDs across the specified statuses."""
    succeeded_set = set(result.success)
    if len(result.success) != len(succeeded_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate Task Instance IDs found in status '{TaskInstanceStatus.SUCCESS}'"
        )

    failed_set = set(result.failed)
    if len(result.failed) != len(failed_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate Task Instance IDs found in status '{TaskInstanceStatus.FAILED}'"
        )

    if duplicates := succeeded_set & failed_set:
        raise RuntimeInconsistencyCheckError(
            f"Duplicate Task Instance IDs found more than one status: {duplicates}"
        )


class Scheduler(LoggingMixing):
    TIMEOUT = 0.1
    MAX_RUN_PER_CYCLE = 10

    _session: Session
    _jop_repo: JobRepository
    _job_run_repo: JobRunRepository
    _ti_repo: TaskInstanceRepository

    def __init__(
        self,
        executor: TaskExecutor,
        create_session: sessionmaker,
        max_parallelism: int = DEFAULT_MAX_PARALLELISM,
    ) -> None:
        super().__init__()
        self._futures: list[Future] = []
        self._max_parallelism = max_parallelism
        self._executor = executor
        self._create_session = create_session

    def __enter__(self) -> Self:
        self._session = self._create_session()
        self._job_repo = JobRepository(self._session)
        self._job_run_repo = JobRunRepository(self._session)
        self._ti_repo = TaskInstanceRepository(self._session)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._session.close()

    def run(self) -> None:
        with self:
            while self._job_repo.count_pending_jobs() > 0 or self._futures:
                if len(self._futures) < self._max_parallelism:
                    scheduled_runs = self.create_pending_job_runs()
                    tis = self._ti_repo.create_task_instances_from_job_runs(
                        scheduled_runs
                    )
                    updated_runs = self._job_run_repo.transition_job_runs_by_state(
                        JobRunStatus.FAILED, JobRunStatus.RUNNING
                    )
                    failed_tis = self._ti_repo.find_task_instances_by_job_run_ids(
                        [run.id for run in updated_runs]
                    )
                    self.execute_tasks(tis + failed_tis)

                results = self.wait_for_completion()
                check_for_inconsistent_task_instances(results)

                success_tis = self._ti_repo.transition_task_instances(
                    results.success, TaskInstanceStatus.SUCCESS
                )
                success_runs = self._job_run_repo.transition_job_runs_state(
                    [ti.job_run for ti in success_tis],
                    JobRunStatus.SUCCESS,
                )
                failed_tis = self._ti_repo.transition_task_instances(
                    results.failed, TaskInstanceStatus.FAILED
                )
                _ = self._job_run_repo.transition_job_runs_state(
                    [ti.job_run for ti in failed_tis],
                    JobRunStatus.FAILED,
                )
                self._job_repo.calculate_next_run(
                    [job_run.job for job_run in success_runs]
                )

    def wait_for_completion(self) -> TasksResults:
        result = TasksResults()
        _completed = set()
        try:
            for future in as_completed(self._futures, timeout=self.TIMEOUT):
                try:
                    _completed.add(future)
                    task = future.result()
                    result.success.append(task.id)
                except TaskExecutionError as e:
                    self.logger.exception(f"Task failed for id={e.task_id}")
                    result.failed.append(e.task_id)
        except TimeoutError:
            num_jobs = len(self._futures)
            self.logger.warning("Timeout exceeded: %d jobs remaining.", num_jobs)
        finally:
            self._futures = [fut for fut in self._futures if fut not in _completed]
            return result

    def create_pending_job_runs(self) -> list[JobRun]:
        jobs = self._job_repo.get_pending_jobs(self.MAX_RUN_PER_CYCLE)
        return self._job_run_repo.create_job_runs_from_jobs(jobs)

    def execute_tasks(self, tasks: list[TaskInstance]) -> None:
        running = [self._executor.submit(task.exchange_data) for task in tasks]
        self._futures.extend(running)
