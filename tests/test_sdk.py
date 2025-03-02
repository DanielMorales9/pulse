from pulse.sdk import JobModel
from pulse.utils import save_yaml

SIMPLE_CONFIG = {
    "name": "random",
    "start_date": "2023-01-01 00:00:00",
    "end_date": "2024-01-01 00:00:00",
    "schedule": "0 0 1 * *",
    "runtime": "subprocess",
    "command": "python -c 'import sys, random; sys.exit(random.choice([0, 1]))'",
}


def test_job_model(tmp_path):
    example_dir = tmp_path / "example"
    example_dir.mkdir()
    example_path = example_dir / "random.yaml"
    save_yaml(example_path, SIMPLE_CONFIG)
    model = JobModel.from_yaml(example_path)
    assert model.name == "random"
    assert model.schedule == "0 0 1 * *"
