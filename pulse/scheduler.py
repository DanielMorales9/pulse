from concurrent.futures import as_completed, Future
from datetime import datetime
from typing import Self, Any

from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM
from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Task, JobRun, JobRunStatus
from pulse.repository import JobRepository, JobRunRepository
from pulse.runtime import TaskExecutionError
from pulse.utils import load_yaml


def _create_task(job_run: JobRun) -> Task:
    job = job_run.job
    obj = load_yaml(job.file_loc)

    rendered_command = obj["command"].format(
        execution_time=job_run.execution_time,
        from_date=job.date_interval_start,
        to_date=job.date_interval_end,
        job_id=job.id,
    )
    runtime = obj["runtime"]
    return Task(
        job_id=job.id, job_run_id=job_run.id, command=rendered_command, runtime=runtime
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
        self._running_jobs: list[Future] = []
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
            while self._job_repo.count_pending_jobs() > 0 or self._running_jobs:
                if len(self._running_jobs) < self._max_parallelism:
                    scheduled_runs = self._schedule_job_runs()
                    tasks = self._create_tasks_from_job_runs(scheduled_runs)
                    self._execute_tasks(tasks)

                success_run_ids, failed_run_ids = self._wait_for_completion()
                complete_job_runs = self._transition_job_runs(
                    success_run_ids, JobRunStatus.SUCCESS
                )
                _ = self._transition_job_runs(failed_run_ids, JobRunStatus.FAILED)
                self._calculate_pending(complete_job_runs)

                self._session.commit()
                self._session.flush()

    def _calculate_pending(self, job_runs: list[JobRun]) -> None:
        ids = {job_run.job_id for job_run in job_runs}
        jobs = self._job_repo.find_jobs_by_ids(ids)
        for job in jobs:
            job.last_run = job.next_run
            job.calculate_next_run()

    def _wait_for_completion(self) -> tuple[list[str], list[str]]:
        success, failed = [], []
        try:
            for future in as_completed(self._running_jobs, timeout=self.TIMEOUT):
                try:
                    task = future.result()
                    success.append(task.job_run_id)
                except TaskExecutionError as e:
                    self.logger.exception("Task failure")
                    failed.append(e.job_run_id)
                finally:
                    self._running_jobs.remove(future)  # TODO this is an anti-pattern
        except TimeoutError:
            self.logger.debug("Timeout exceeded")
        finally:
            return success, failed

    def _transition_job_runs(
        self, job_run_ids: list[str], status: JobRunStatus
    ) -> list[JobRun]:
        job_runs = []
        for job_run in self._job_run_repo.find_job_runs_by_ids(job_run_ids):
            job_run.status = status
            job_run.retry_number = job_run.retry_number + 1
            job_runs.append(job_run)
        return job_runs

    def _schedule_job_runs(self) -> list[JobRun]:
        job_runs = []
        at = datetime.utcnow()
        runs = self._job_run_repo.find_job_runs_in_states(
            (JobRunStatus.RUNNING, JobRunStatus.FAILED)
        )
        ids = {run.job_id for run in runs}
        jobs = self._job_repo.select_jobs_for_execution(ids, self.MAX_RUN_PER_CYCLE)
        for job in jobs:
            job_run = self._job_run_repo.create_run_from_job(job, at)
            job_runs.append(job_run)
        self._session.add_all(job_runs)
        self._session.flush()

        failed_runs = self._job_run_repo.find_job_runs_by_state(JobRunStatus.FAILED)
        return job_runs + failed_runs

    @staticmethod
    def _create_tasks_from_job_runs(job_runs: list[JobRun]) -> list[Task]:
        return [_create_task(job_run) for job_run in job_runs]

    def _execute_tasks(self, tasks: list[Task]) -> None:
        running = [self._executor.submit(task) for task in tasks]
        self._running_jobs.extend(running)
