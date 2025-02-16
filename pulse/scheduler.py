from typing import Self, Any

from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM, JobRunStatus, TaskInstanceStatus, EnvEnum, ENV
from pulse.logutils import LoggingMixing
from pulse.models import JobRun, TaskInstance, TaskResultStatus, TaskResult
from pulse.task_queue import TaskQueue
from pulse.repository import (
    JobRepository,
    JobRunRepository,
    TaskInstanceRepository,
)


class RuntimeInconsistencyCheckError(Exception):
    """Raised when an inconsistency is found during a runtime consistency check."""


def check_for_inconsistent_task_instances(
    success: list[str], failed: list[str]
) -> None:
    """Checks for inconsistent task instances results, such as duplicate IDs across the specified statuses."""
    succeeded_set = set(success)
    if len(success) != len(succeeded_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate Task Instance IDs found in status '{TaskResultStatus.SUCCESS}'"
        )

    failed_set = set(failed)
    if len(failed) != len(failed_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate Task Instance IDs found in status '{TaskResultStatus.FAILED}'"
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
        create_session: sessionmaker,
        task_queue: TaskQueue,
        max_parallelism: int = DEFAULT_MAX_PARALLELISM,
    ) -> None:
        super().__init__()
        self._max_parallelism = max_parallelism
        self._create_session = create_session
        self._task_queue = task_queue

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
            while not self.stopping_criteria_met():
                if self._task_queue.size() < self._max_parallelism:
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
                success = list(
                    el.id for el in results if el.status == TaskResultStatus.SUCCESS
                )
                failed = list(
                    el.id for el in results if el.status == TaskResultStatus.FAILED
                )
                check_for_inconsistent_task_instances(success, failed)

                success_tis = self._ti_repo.transition_task_instances(
                    success, TaskInstanceStatus.SUCCESS
                )
                success_runs = self._job_run_repo.transition_job_runs_state(
                    [ti.job_run for ti in success_tis],
                    JobRunStatus.SUCCESS,
                )
                failed_tis = self._ti_repo.transition_task_instances(
                    failed, TaskInstanceStatus.FAILED
                )
                _ = self._job_run_repo.transition_job_runs_state(
                    [ti.job_run for ti in failed_tis],
                    JobRunStatus.FAILED,
                )
                self._job_repo.calculate_next_run(
                    [job_run.job for job_run in success_runs]
                )

    def stopping_criteria_met(self) -> bool:
        return (
                ENV == EnvEnum.TEST
                and self._job_repo.count_pending_jobs() == 0
                and self._task_queue.size() == 0
        )

    def wait_for_completion(self) -> list[TaskResult]:
        return list(self._task_queue.receive(self.TIMEOUT))

    def create_pending_job_runs(self) -> list[JobRun]:
        jobs = self._job_repo.get_pending_jobs(self.MAX_RUN_PER_CYCLE)
        return self._job_run_repo.create_job_runs_from_jobs(jobs)

    def execute_tasks(self, tasks: list[TaskInstance]) -> None:
        for task in tasks:
            self._task_queue.send(task.exchange_data)
