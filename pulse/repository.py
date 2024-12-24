from datetime import datetime
from typing import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from pulse.models import Job, JobRunStatus, JobRun


class JobRunRepository:
    @staticmethod
    def find_job_runs_by_ids(ids: list[str], session: Session) -> list[JobRun]:
        return session.query(JobRun).filter(JobRun.id.in_(ids)).all()

    @staticmethod
    def find_job_runs_by_state(state: JobRunStatus, session: Session) -> list[JobRun]:
        return session.query(JobRun).filter(JobRun.status == state).all()

    @staticmethod
    def find_job_runs_in_states(
        states: Iterable[JobRunStatus], session: Session
    ) -> list[JobRun]:
        return session.query(JobRun).filter(JobRun.status.in_(states)).all()

    @staticmethod
    def create_run_from_job(job: Job, execution_time: datetime) -> JobRun:
        return JobRun(
            job_id=job.id,
            status=JobRunStatus.RUNNING,
            date_interval_start=job.date_interval_start,
            date_interval_end=job.date_interval_end,
            execution_time=execution_time,
            retry_number=0,
        )


class JobRepository:
    @staticmethod
    def select_jobs_for_execution(session: Session, limit: int) -> list[Job]:
        states = (JobRunStatus.RUNNING, JobRunStatus.FAILED)
        runs = JobRunRepository.find_job_runs_in_states(states, session)
        running_or_failed_ids = {run.job_id for run in runs}
        return (
            session.query(Job)
            .filter(func.now() >= Job.next_run)
            .filter(Job.id.not_in(running_or_failed_ids))
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
