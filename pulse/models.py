import dataclasses
from datetime import datetime
from pathlib import Path

from croniter import croniter
from sqlalchemy import Integer, Column, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

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

    id = Column(Integer, nullable=False, primary_key=True)
    file_loc = Column(String, nullable=False)
    schedule = Column(Text, nullable=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    last_run = Column(DateTime, nullable=True)

    def __init__(
        self,
        id: int,
        file_loc: str,
        schedule: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.id = id
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


@dataclasses.dataclass
class Task:
    job_id: int
    command: str
    runtime: RuntimeType
