from dataclasses import field, dataclass
from concurrent.futures import as_completed, Future
from datetime import datetime
from typing import Self, Any

from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM, JobRunStatus
from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Task, JobRun

from pulse.repository import JobRepository, JobRunRepository
from pulse.runtime import TaskExecutionError
from pulse.utils import load_yaml


class RuntimeInconsistencyCheckError(Exception):
    """Raised when an inconsistency is found during a runtime consistency check."""


def _create_task(job_run: JobRun) -> Task:
    job = job_run.job
    obj = load_yaml(job.file_loc)

    rendered_command = obj["command"].format(
        execution_time=job_run.execution_time,
        from_date=job_run.date_interval_start,
        to_date=job_run.date_interval_end,
        job_id=job.id,
    )
    runtime = obj["runtime"]
    return Task(
        job_id=job.id, job_run_id=job_run.id, command=rendered_command, runtime=runtime
    )


def list_factory() -> list[str]:
    return []


@dataclass(frozen=True)
class TasksResults:
    success: list[str] = field(default_factory=list_factory)
    failed: list[str] = field(default_factory=list_factory)


def check_for_inconsistent_job_runs(result: TasksResults) -> None:
    """Checks for inconsistencies in job results, such as duplicate job run IDs across the specified statuses."""
    succeeded_set = set(result.success)
    if len(result.success) != len(succeeded_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate job run IDs found in status '{JobRunStatus.SUCCESS}'"
        )

    failed_set = set(result.failed)
    if len(result.failed) != len(failed_set):
        raise RuntimeInconsistencyCheckError(
            f"Duplicate job run IDs found in status '{JobRunStatus.FAILED}'"
        )

    if duplicates := succeeded_set & failed_set:
        raise RuntimeInconsistencyCheckError(
            f"Duplicate job run IDs found more than one status: {duplicates}"
        )


class Scheduler(LoggingMixing):
    TIMEOUT = 0.1
    MAX_RUN_PER_CYCLE = 10

    _session: Session
    _jop_repo: JobRepository
    _job_run_repo: JobRunRepository

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
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._session.close()

    def run(self) -> None:
        with self:
            while self._job_repo.count_pending_jobs() > 0 or self._futures:
                if len(self._futures) < self._max_parallelism:
                    scheduled_runs = self.create_pending_job_runs()
                    updated_runs = self._job_run_repo.transition_job_runs_by_state(
                        JobRunStatus.FAILED, JobRunStatus.RUNNING
                    )
                    tasks = self._create_tasks_from_job_runs(
                        scheduled_runs + updated_runs
                    )
                    self._execute_tasks(tasks)

                results = self.wait_for_completion()
                check_for_inconsistent_job_runs(results)

                success = self._job_run_repo.transition_job_runs(
                    results.success,
                    JobRunStatus.SUCCESS,
                )
                _ = self._job_run_repo.transition_job_runs(
                    results.failed, JobRunStatus.FAILED
                )
                self._job_repo.calculate_next_run([job_run.job for job_run in success])

    def wait_for_completion(self) -> TasksResults:
        result = TasksResults()
        _completed = set()
        try:
            for future in as_completed(self._futures, timeout=self.TIMEOUT):
                try:
                    _completed.add(future)
                    task = future.result()
                    result.success.append(task.job_run_id)
                except TaskExecutionError as e:
                    self.logger.exception(f"Task failed for id={e.job_run_id}")
                    result.failed.append(e.job_run_id)
        except TimeoutError:
            num_jobs = len(self._futures)
            self.logger.warning("Timeout exceeded: %d jobs remaining.", num_jobs)
        finally:
            self._futures = [fut for fut in self._futures if fut not in _completed]
            return result

    def create_pending_job_runs(self) -> list[JobRun]:
        at = datetime.utcnow()
        jobs = self._job_repo.get_pending_jobs(self.MAX_RUN_PER_CYCLE)
        job_runs = [self._job_run_repo.create_run_from_job(job, at) for job in jobs]
        self._session.add_all(job_runs)
        self._session.commit()
        return job_runs

    @staticmethod
    def _create_tasks_from_job_runs(job_runs: list[JobRun]) -> list[Task]:
        return [_create_task(job_run) for job_run in job_runs]

    def _execute_tasks(self, tasks: list[Task]) -> None:
        running = [self._executor.submit(task) for task in tasks]
        self._futures.extend(running)
