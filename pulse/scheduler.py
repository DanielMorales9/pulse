from concurrent.futures import as_completed, Future
from datetime import datetime

import yaml
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM
from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Job, Task, JobRun, JobRunStatus


class JobRepository:
    @staticmethod
    def select_jobs_for_execution(session: Session, limit: int) -> list[Job]:
        running_ids = set(
            session.query(JobRun.job_id)
            .filter(JobRun.status == JobRunStatus.RUNNING)
            .all()
        )
        return (
            session.query(Job)
            .filter(func.now() >= Job.next_run)
            .filter(Job.id.not_in(running_ids))
            .order_by(Job.next_run)
            .limit(limit)
            .all()
        )

    @staticmethod
    def count_pending_jobs(session: Session) -> int:
        return session.query(Job).filter(Job.next_run.isnot(None)).count()

    @staticmethod
    def find_jobs_by_ids(ids: list[str], session: Session) -> list[Job]:
        return session.query(Job).filter(Job.id.in_(ids)).all()


class JobRunRepository:
    @staticmethod
    def find_jobs_by_ids(ids: list[str], session: Session) -> list[JobRun]:
        return session.query(JobRun).filter(JobRun.id.in_(ids)).all()


def _create_task(job_run: JobRun) -> Task:
    job = job_run.job
    with open(job.file_loc) as f:
        obj = yaml.safe_load(f)

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


def _create_run_from_job(job: Job, execution_time: datetime) -> JobRun:
    return JobRun(
        job_id=job.id,
        status=JobRunStatus.RUNNING,
        date_interval_start=job.date_interval_start,
        date_interval_end=job.date_interval_end,
        execution_time=execution_time,
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

                completed_tasks = self._wait_for_completion()
                job_run_ids = [task.job_run_id for task in completed_tasks]
                complete_job_runs = self._complete_job_runs(job_run_ids, session)
                self._calculate_pending(complete_job_runs)

                session.commit()
                session.flush()

    @staticmethod
    def _calculate_pending(job_runs: list[JobRun]) -> None:
        for job_run in job_runs:
            job = job_run.job
            job.last_run = job.next_run
            job.calculate_next_run()

    def _wait_for_completion(self) -> list[Task]:
        tasks = []
        try:
            for future in as_completed(self._running_jobs, timeout=self.TIMEOUT):
                task = future.result()
                tasks.append(task)
                self._running_jobs.remove(future)
        except TimeoutError:
            self.logger.debug("Timeout exceeded")
        finally:
            return tasks

    @staticmethod
    def _complete_job_runs(job_run_ids: list[str], session: Session) -> list[JobRun]:
        job_runs = []
        for job_run in JobRunRepository.find_jobs_by_ids(job_run_ids, session):
            job_run.status = JobRunStatus.COMPLETED
            job_runs.append(job_run)
        return job_runs

    def _schedule_job_runs(self, session: Session) -> list[JobRun]:
        job_runs = []
        at = datetime.utcnow()
        for job in JobRepository.select_jobs_for_execution(
            session, self.MAX_RUN_PER_CYCLE
        ):
            job_runs.append(_create_run_from_job(job, at))
        session.add_all(job_runs)
        session.flush()
        return job_runs

    @staticmethod
    def _create_tasks_from_job_runs(job_runs: list[JobRun]) -> list[Task]:
        return [_create_task(job_run) for job_run in job_runs]

    def _execute_tasks(self, tasks: list[Task]) -> None:
        running = [self._executor.submit(task) for task in tasks]
        self._running_jobs.extend(running)
