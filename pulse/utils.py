from pathlib import Path
from typing import Any

import yaml


def create_job_file(temp_file_path: Path | str, data: dict[str, Any]) -> Path:
    with open(temp_file_path, mode="w") as f:
        yaml.safe_dump(data, f)
    return Path(temp_file_path)
