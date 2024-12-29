from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

from pulse.utils import get_cron_next_value

if TYPE_CHECKING:
    from pulse.models import Job


class ATimetable(ABC):
    @abstractmethod
    def calculate(self, job: "Job") -> datetime | None:
        pass


class CronTimetable(ATimetable):
    def calculate(self, job: "Job") -> datetime | None:
        if job.end_date and job.next_run and job.next_run >= job.end_date:
            return None
        base = job.last_run or job.start_date
        return get_cron_next_value(job.schedule, base)  # type: ignore[arg-type]


class OnceTimetable(ATimetable):
    def calculate(self, job: "Job") -> datetime | None:
        if job.last_run:
            return None
        return job.start_date


def create_timetable(schedule: str | None) -> ATimetable:
    return CronTimetable() if schedule else OnceTimetable()
