import dataclasses
from datetime import datetime

from croniter import croniter

from pulse.constants import RuntimeType


def get_cron_next_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_next(datetime)  # type: ignore[no-any-return]


def get_cron_prev_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_prev(datetime)  # type: ignore[no-any-return]


@dataclasses.dataclass
class Job:
    id: int
    command: str
    runtime: RuntimeType
    schedule: str | None
    start_date: datetime
    end_date: datetime | None
    next_run: datetime
    last_run: datetime | None = None  # will be replaced by job run

    def __init__(
        self,
        id: int,
        command: str,
        runtime: RuntimeType,
        schedule: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.id = id
        self.command = command
        self.runtime = runtime
        self.schedule = schedule
        self.start_date = start_date or datetime.utcnow()
        self.end_date = end_date
        self.calculate_next_run()

    @property
    def completed(self) -> bool:
        if not self.next_run:
            return False

        if not self.schedule:
            return self.last_run is not None

        if self.end_date:
            return self.next_run > self.end_date
        return False

    def calculate_next_run(self) -> None:
        if not self.schedule:
            self.next_run = self.start_date
            return
        base = self.last_run or self.start_date
        self.next_run = get_cron_next_value(self.schedule, base)

    @property
    def date_interval_start(self) -> datetime:
        if not self.schedule:
            return self.date_interval_end
        return get_cron_prev_value(self.schedule, self.date_interval_end)

    @property
    def date_interval_end(self) -> datetime:
        return self.next_run

    def __repr__(self) -> str:
        fields = ", ".join(
            f"{field.name}={value}"
            for field in dataclasses.fields(self)
            if (value := getattr(self, field.name)) is not None
        )
        return f"Job({fields})"


@dataclasses.dataclass
class Task:
    job_id: int
    command: str
    runtime: RuntimeType
