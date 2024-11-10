import dataclasses
from datetime import datetime

from croniter import croniter

from pulse.constants import RuntimeType


@dataclasses.dataclass
class Job:
    id: int
    command: str
    runtime: RuntimeType
    schedule: str | None = None
    next_run: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None

    @property
    def completed(self) -> bool:
        if not self.next_run:
            return False

        if self.schedule and self.end_date:
            return self.next_run > self.end_date

        return not self.schedule and self.next_run is not None

    def calculate_next_run(self, at: datetime) -> datetime:
        if not self.schedule:
            return at
        cron = croniter(self.schedule, at)
        return cron.get_next(datetime)  # type: ignore[no-any-return]

    @property
    def prev_run(self) -> datetime:
        assert self.next_run
        if not self.schedule:
            return self.next_run
        cron = croniter(self.schedule, self.next_run)
        return cron.get_prev(datetime)  # type: ignore[no-any-return]

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
