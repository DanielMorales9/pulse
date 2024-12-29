from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    Enum,
    Integer,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship, Mapped

from pulse.constants import (
    RuntimeType,
    JobRunStatus,
    DEFAULT_SCHEDULE,
    TaskInstanceStatus,
)
from pulse.timetable import create_timetable
from pulse.utils import get_cron_prev_value, uuid4_gen

Base = declarative_base()


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = Column(
        String,
        primary_key=True,
        default=uuid4_gen,
        nullable=False,
    )
    file_loc: Mapped[str] = Column(String, nullable=False)
    schedule: Mapped[str | None] = Column(Text, nullable=True)
    start_date: Mapped[datetime] = Column(DateTime, nullable=False)
    end_date: Mapped[datetime | None] = Column(DateTime, nullable=True)
    next_run: Mapped[datetime | None] = Column(DateTime, nullable=True)
    last_run: Mapped[datetime | None] = Column(DateTime, nullable=True)
    date_interval_start: Mapped[datetime | None] = Column(DateTime, nullable=True)
    date_interval_end: Mapped[datetime | None] = Column(DateTime, nullable=True)

    def __init__(
        self,
        file_loc: str,
        schedule: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.file_loc = file_loc
        self.schedule = schedule
        self.start_date = start_date or datetime.utcnow()
        self.end_date = end_date
        calculate_next_run(self)


def calculate_next_run(job: Job) -> None:
    timetable = create_timetable(job.schedule)
    job.next_run = timetable.calculate(job)
    if not job.next_run:
        return
    schedule = job.schedule or DEFAULT_SCHEDULE
    job.date_interval_start = get_cron_prev_value(schedule, job.next_run)
    job.date_interval_end = job.next_run


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[str] = Column(
        String,
        primary_key=True,
        default=uuid4_gen,
        nullable=False,
    )
    job_id: Mapped[str] = Column(String, ForeignKey("jobs.id"), nullable=False)
    status: Mapped[JobRunStatus] = Column(Enum(JobRunStatus), nullable=False)
    date_interval_start: Mapped[datetime] = Column(DateTime, nullable=False)
    date_interval_end: Mapped[datetime] = Column(DateTime, nullable=True)
    execution_time: Mapped[datetime] = Column(DateTime, nullable=True)
    retry_number: Mapped[int] = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("job_id", "date_interval_start", name="uix_run"),
    )

    job: Mapped[Job] = relationship("Job")


@dataclass(frozen=True, kw_only=True)
class Task:
    id: str
    command: str
    runtime: RuntimeType


class TaskInstance(Base):
    __tablename__ = "task_instances"

    id: Mapped[str] = Column(
        String,
        primary_key=True,
        default=uuid4_gen,
        nullable=False,
    )
    job_run_id: Mapped[str] = Column(String, ForeignKey("job_runs.id"), nullable=False)
    status: Mapped[TaskInstanceStatus] = Column(
        Enum(TaskInstanceStatus), nullable=False
    )
    command: Mapped[str] = Column(String, nullable=False)
    runtime: Mapped[RuntimeType] = Column(Enum(RuntimeType), nullable=False)
    retry_number: Mapped[int] = Column(Integer, nullable=False, default=0)

    job_run: Mapped[JobRun] = relationship("JobRun")

    @property
    def rendered_command(self) -> str:
        return self.command.format(
            execution_time=self.job_run.execution_time,
            from_date=self.job_run.date_interval_start,
            to_date=self.job_run.date_interval_end,
        )

    @property
    def exchange_data(self) -> Task:
        return Task(
            id=self.id,
            command=self.rendered_command,
            runtime=self.runtime,
        )
