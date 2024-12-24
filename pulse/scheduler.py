from concurrent.futures import as_completed, Future
from datetime import datetime

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

    def run(self) -> None:
        with self._create_session() as session:
            while JobRepository.count_pending_jobs(session) > 0 or self._running_jobs:
                if len(self._running_jobs) < self._max_parallelism:
                    scheduled_runs = self._schedule_job_runs(session)
                    tasks = self._create_tasks_from_job_runs(scheduled_runs)
                    self._execute_tasks(tasks)

                success_run_ids, failed_run_ids = self._wait_for_completion()
                complete_job_runs = self._succeed_job_runs(success_run_ids, session)
                _ = self._fail_job_runs(failed_run_ids, session)
                self._calculate_pending(complete_job_runs)

                session.commit()
                session.flush()

    @staticmethod
    def _calculate_pending(job_runs: list[JobRun]) -> None:
        for job_run in job_runs:
            job = job_run.job
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

    @staticmethod
    def _transition_job_runs(
        job_run_ids: list[str], status: JobRunStatus, session: Session
    ) -> list[JobRun]:
        job_runs = []
        for job_run in JobRunRepository.find_job_runs_by_ids(job_run_ids, session):
            job_run.status = status
            job_run.retry_number = job_run.retry_number + 1
            job_runs.append(job_run)
        return job_runs

    @staticmethod
    def _succeed_job_runs(job_run_ids: list[str], session: Session) -> list[JobRun]:
        return Scheduler._transition_job_runs(
            job_run_ids, JobRunStatus.SUCCESS, session
        )

    @staticmethod
    def _fail_job_runs(job_run_ids: list[str], session: Session) -> list[JobRun]:
        return Scheduler._transition_job_runs(job_run_ids, JobRunStatus.FAILED, session)

    def _schedule_job_runs(self, session: Session) -> list[JobRun]:
        job_runs = []
        at = datetime.utcnow()
        for job in JobRepository.select_jobs_for_execution(
            session, self.MAX_RUN_PER_CYCLE
        ):
            job_run = JobRunRepository.create_run_from_job(job, at)
            job_runs.append(job_run)
        session.add_all(job_runs)
        session.flush()

        failed_runs = JobRunRepository.find_job_runs_by_state(
            JobRunStatus.FAILED, session
        )
        return job_runs + failed_runs

    @staticmethod
    def _create_tasks_from_job_runs(job_runs: list[JobRun]) -> list[Task]:
        return [_create_task(job_run) for job_run in job_runs]

    def _execute_tasks(self, tasks: list[Task]) -> None:
        running = [self._executor.submit(task) for task in tasks]
        self._running_jobs.extend(running)
