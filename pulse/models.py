import dataclasses
import uuid
from datetime import datetime
from enum import StrEnum

from croniter import croniter
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Enum, UUID  # type: ignore[attr-defined]
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from pulse.constants import RuntimeType


def get_cron_next_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_next(datetime)  # type: ignore[no-any-return]


def get_cron_prev_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_prev(datetime)  # type: ignore[no-any-return]


Base = declarative_base()


class Job(Base):
    __tablename__ = "jobs"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    file_loc = Column(String, nullable=False)
    schedule = Column(Text, nullable=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    last_run = Column(DateTime, nullable=True)

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
        self.next_run = None
        self.calculate_next_run()

    def calculate_next_run(self) -> None:
        if not self.schedule and not self.last_run:
            self.next_run = self.start_date
        elif not self.schedule:
            self.next_run = None
        elif self.end_date and self.next_run and self.next_run >= self.end_date:
            self.next_run = None
        else:
            base = self.last_run or self.start_date
            self.next_run = get_cron_next_value(self.schedule, base)

    @property
    def date_interval_start(self) -> datetime:
        if not self.schedule:
            return self.date_interval_end
        return get_cron_prev_value(self.schedule, self.date_interval_end)

    @property
    def date_interval_end(self) -> datetime:
        assert self.next_run
        return self.next_run


class JobRunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"


class JobRun(Base):
    __tablename__ = "job_runs"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    status = Column(Enum(JobRunStatus), nullable=False)
    date_interval_start = Column(DateTime, nullable=False)
    date_interval_end = Column(DateTime, nullable=True)
    execution_time = Column(DateTime, nullable=True)

    job = relationship("Job")


@dataclasses.dataclass
class Task:
    job_id: str
    job_run_id: str
    command: str
    runtime: RuntimeType
