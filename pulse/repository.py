from datetime import datetime
from typing import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from pulse.models import Job, JobRunStatus, JobRun


class JobRunRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def find_job_runs_by_ids(self, ids: list[str]) -> list[JobRun]:
        return self._session.query(JobRun).filter(JobRun.id.in_(ids)).all()

    def find_job_runs_by_state(self, state: JobRunStatus) -> list[JobRun]:
        return self._session.query(JobRun).filter(JobRun.status == state).all()

    def find_job_runs_in_states(self, states: Iterable[JobRunStatus]) -> list[JobRun]:
        return self._session.query(JobRun).filter(JobRun.status.in_(states)).all()

    @classmethod
    def create_run_from_job(cls, job: Job, execution_time: datetime) -> JobRun:
        return JobRun(
            job_id=job.id,
            status=JobRunStatus.RUNNING,
            date_interval_start=job.date_interval_start,
            date_interval_end=job.date_interval_end,
            execution_time=execution_time,
            retry_number=0,
        )


class JobRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def select_jobs_for_execution(
        self, job_ids: Iterable[str], limit: int
    ) -> list[Job]:
        return (
            self._session.query(Job)
            .filter(func.now() >= Job.next_run)
            .filter(Job.id.not_in(job_ids))
            .order_by(Job.next_run)
            .limit(limit)
            .all()
        )

    def count_pending_jobs(self) -> int:
        return self._session.query(Job).filter(Job.next_run.isnot(None)).count()

    def find_jobs_by_ids(self, ids: Iterable[str]) -> list[Job]:
        return self._session.query(Job).filter(Job.id.in_(ids)).all()
