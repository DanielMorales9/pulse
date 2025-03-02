from datetime import datetime
from pathlib import Path
from typing import Self

from pydantic import BaseModel

from pulse.constants import RuntimeType
from pulse.utils import load_yaml


class JobModel(BaseModel):
    file_path: Path
    name: str
    start_date: datetime
    end_date: datetime
    schedule: str
    runtime: RuntimeType
    command: str

    @staticmethod
    def from_yaml(path: Path) -> "JobModel":
        obj = load_yaml(path)
        return JobModel(file_path=path, **obj)
