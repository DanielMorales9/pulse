from concurrent.futures import as_completed, Future
from datetime import datetime

import yaml
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import DEFAULT_MAX_PARALLELISM
from pulse.executor import TaskExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Job, Task


def create_task(job: Job, execution_time: datetime) -> Task:
    with open(job.file_loc) as f:
        obj = yaml.safe_load(f)

    rendered_command = obj["command"].format(
        execution_time=execution_time,
        from_date=job.date_interval_start,
        to_date=job.date_interval_end,
        job_id=job.id,
    )
    return Task(job_id=job.id, command=rendered_command, runtime=obj["runtime"])


class JobRepository:
    @staticmethod
    def select_jobs_for_execution(session: Session, limit: int) -> list[Job]:
        return (
            session.query(Job)
            .filter(func.now() >= Job.next_run)
            .order_by(Job.next_run)
            .limit(limit)
            .all()
        )

    @staticmethod
    def count_pending_jobs(session: Session) -> int:
        return session.query(Job).filter(Job.next_run.isnot(None)).count()

    @staticmethod
    def find_jobs_by_ids(ids: list[int], session: Session) -> list[Job]:
        return session.query(Job).filter(Job.id.in_(ids)).all()


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

    def initialize(self, jobs: list[Job]) -> None:
        with self._create_session() as session:
            session.add_all(jobs)
            session.commit()

    def run(self) -> None:
        with self._create_session() as session:
            while JobRepository.count_pending_jobs(session) > 0 or self._running_jobs:
                at = datetime.utcnow()
                if len(self._running_jobs) < self._max_parallelism:
                    scheduled = self._schedule_pending(at, session)
                    self._running_jobs.extend(scheduled)
                completed = self._wait_for_completion()
                self._calculate_pending(completed, session)
                session.commit()
                session.flush()

    @staticmethod
    def _calculate_pending(ids: list[int], session: Session) -> None:
        for job in JobRepository.find_jobs_by_ids(ids, session):
            job.last_run = job.next_run
            job.calculate_next_run()

    def _wait_for_completion(self) -> list[int]:
        completed_jobs = []
        try:
            for future in as_completed(self._running_jobs, timeout=self.TIMEOUT):
                task = future.result()
                completed_jobs.append(task.job_id)
                self._running_jobs.remove(future)
        except TimeoutError:
            self.logger.debug("Timeout exceeded")
        finally:
            return completed_jobs

    def _schedule_pending(self, at: datetime, session: Session) -> list[Future]:
        result = []
        for job in JobRepository.select_jobs_for_execution(
            session, self.MAX_RUN_PER_CYCLE
        ):
            task = create_task(job, at)
            future = self._executor.submit(task)
            result.append(future)
        return result
