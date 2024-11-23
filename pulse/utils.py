from pathlib import Path
from typing import Any

import yaml


def save_yaml(file_path: Path | str, obj: dict[str, Any]) -> None:
    with open(file_path, mode="w") as f:
        yaml.safe_dump(obj, f)
