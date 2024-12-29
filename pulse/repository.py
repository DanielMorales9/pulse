from datetime import datetime
from typing import Iterable, Sequence

from sqlalchemy import func, select, exists
from sqlalchemy.orm import Session

from pulse.models import Job, JobRun, TaskInstance, calculate_next_run
from pulse.constants import UNFINISHED_JOB_RUN_STATES, JobRunStatus, TaskInstanceStatus
from pulse.utils import load_yaml


class JobRunRepository:
    RETRY_INCREMENT_BY_STATE = {JobRunStatus.FAILED: 1}

    def __init__(self, session: Session) -> None:
        self._session = session

    def find_job_runs_by_ids(self, ids: list[str]) -> list[JobRun]:
        return self._session.query(JobRun).filter(JobRun.id.in_(ids)).all()

    def find_job_runs_by_state(self, state: JobRunStatus) -> list[JobRun]:
        return self._session.query(JobRun).filter(JobRun.status == state).all()

    def transition_job_runs(
        self, job_run_ids: list[str], status: JobRunStatus
    ) -> list[JobRun]:
        job_runs = self.find_job_runs_by_ids(job_run_ids)
        return self.transition_job_runs_state(job_runs, status)

    def transition_job_runs_by_state(
        self, from_status: JobRunStatus, to_status: JobRunStatus
    ) -> list[JobRun]:
        job_runs = self.find_job_runs_by_state(from_status)
        return self.transition_job_runs_state(job_runs, to_status)

    def transition_job_runs_state(
        self, job_runs: list[JobRun], to_status: JobRunStatus
    ) -> list[JobRun]:
        for job_run in job_runs:
            job_run.status = to_status
            job_run.retry_number += self.RETRY_INCREMENT_BY_STATE.get(to_status, 0)
        self._session.commit()
        return job_runs

    @classmethod
    def create_job_run_from_job(cls, job: Job, execution_time: datetime) -> JobRun:
        return JobRun(
            job_id=job.id,
            status=JobRunStatus.RUNNING,
            date_interval_start=job.date_interval_start,  # type: ignore[arg-type]
            date_interval_end=job.date_interval_end,  # type: ignore[arg-type]
            execution_time=execution_time,
        )

    def create_job_runs_from_jobs(self, jobs: list[Job]) -> list[JobRun]:
        execution_time = datetime.utcnow()
        job_runs = [self.create_job_run_from_job(job, execution_time) for job in jobs]
        self._session.add_all(job_runs)
        self._session.commit()
        return job_runs


class JobRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_pending_jobs(self, limit: int) -> list[Job]:
        stmt = select(JobRun.job_id).where(JobRun.status.in_(UNFINISHED_JOB_RUN_STATES))
        job_run_exists = exists(stmt.where(JobRun.job_id == Job.id))

        return (
            self._session.query(Job)
            .filter(func.now() >= Job.next_run)
            .filter(~job_run_exists)
            .order_by(Job.next_run)
            .limit(limit)
            .all()
        )

    def count_pending_jobs(self) -> int:
        return self._session.query(Job).filter(Job.next_run.isnot(None)).count()

    def find_jobs_by_ids(self, ids: Iterable[str]) -> list[Job]:
        return self._session.query(Job).filter(Job.id.in_(ids)).all()

    def calculate_next_run(self, jobs: list[Job]) -> None:
        for job in jobs:
            if not job.next_run:
                continue
            job.last_run = job.next_run
            # TODO better design
            calculate_next_run(job)
        self._session.commit()


class TaskInstanceRepository:
    RETRY_INCREMENT_BY_STATE = {TaskInstanceStatus.FAILED: 1}

    def __init__(self, session: Session) -> None:
        self._session = session

    def find_task_instances_by_job_run_ids(self, ids: list[str]) -> list[TaskInstance]:
        return (
            self._session.query(TaskInstance)
            .filter(TaskInstance.job_run_id.in_(ids))
            .all()
        )

    def find_task_instances_by_ids(self, ids: list[str]) -> list[TaskInstance]:
        return self._session.query(TaskInstance).filter(TaskInstance.id.in_(ids)).all()

    def transition_task_instances_state(
        self, tis: list[TaskInstance], status: TaskInstanceStatus
    ) -> list[TaskInstance]:
        for ti in tis:
            ti.status = status
            ti.retry_number += self.RETRY_INCREMENT_BY_STATE.get(status, 0)
        self._session.commit()
        return tis

    def transition_task_instances(
        self, ti_ids: list[str], status: TaskInstanceStatus
    ) -> list[TaskInstance]:
        job_runs = self.find_task_instances_by_ids(ti_ids)
        return self.transition_task_instances_state(job_runs, status)

    @staticmethod
    def create_task_instance_from_job_run(job_run: JobRun) -> TaskInstance:
        obj = load_yaml(job_run.job.file_loc)
        return TaskInstance(
            job_run_id=job_run.id, status=TaskInstanceStatus.RUNNING, **obj
        )

    def create_task_instances_from_job_runs(
        self, job_runs: list[JobRun]
    ) -> list[TaskInstance]:
        tis = [self.create_task_instance_from_job_run(job_run) for job_run in job_runs]
        self._session.add_all(tis)
        self._session.commit()
        return tis
