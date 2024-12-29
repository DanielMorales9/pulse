import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter


def save_yaml(file_path: Path | str, obj: dict[str, Any]) -> None:
    with open(file_path, mode="w") as f:
        yaml.safe_dump(obj, f)


def load_yaml(file_path: Path | str) -> Any:
    with open(file_path) as f:
        obj = yaml.safe_load(f)
    return obj


def get_cron_next_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_next(datetime)  # type: ignore[no-any-return]


def get_cron_prev_value(expression: str, at: datetime) -> datetime:
    cron = croniter(expression, at)
    return cron.get_prev(datetime)  # type: ignore[no-any-return]


def uuid4_gen() -> str:
    return str(uuid.uuid4())
