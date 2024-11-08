import dataclasses
from datetime import datetime

from croniter import croniter

from pulse.constants import RuntimeType


@dataclasses.dataclass
class Job:
    command: str | list[str]
    runtime: RuntimeType
    schedule: str | None = None
    next_run: datetime | None = None

    def calculate_next_run(self, at: datetime) -> datetime:
        if not self.schedule:
            return at
        cron = croniter(self.schedule, at)
        return cron.get_next(datetime)  # type: ignore[no-any-return]
