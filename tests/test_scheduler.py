import datetime
from concurrent.futures import Future
from unittest.mock import patch

import pytest

from pulse.constants import RuntimeType
from pulse.scheduler import (
    Scheduler,
)
from pulse.models import Job, get_cron_next_value
from pulse.utils import create_job_file


def _set_result(x):
    future = Future()
    future.set_result(x)
    return future


@pytest.fixture
def mock_job(request, tmp_path):
    obj = request.param
    file_path = tmp_path / "job.yaml"
    file_path.touch()
    create_job_file(file_path, obj)
    yield Job(id=0, file_loc=file_path)


@pytest.mark.parametrize(
    "mock_job",
    [
        dict(command="echo 'hello world'", runtime=RuntimeType.SUBPROCESS.value),
        dict(command="echo 'hello world'", runtime=RuntimeType.DOCKER.value),
    ],
    indirect=True,
)
def test_scheduler_run(mock_job, mock_executor):
    scheduler = Scheduler(mock_executor)
    scheduler.initialize([mock_job])
    scheduler.run()
    assert mock_executor.submit.called


@pytest.mark.parametrize(
    "expression, expected",
    [
        (
            "* * * * *",
            datetime.datetime(2020, 1, 1, 0, 1),
        ),
        (
            "0 * * * *",
            datetime.datetime(2020, 1, 1, 1, 0),
        ),
    ],
)
def test_get_cron_next_value(expression, expected):
    at = datetime.datetime(2020, 1, 1)
    assert get_cron_next_value(expression, at) == expected


@patch("pulse.scheduler.datetime")
def test_scheduler_loop(mock_datetime, mock_executor, tmp_path):
    obj = dict(command="echo 'hello world'", runtime=RuntimeType.SUBPROCESS.value)
    file_path = tmp_path / "job.yaml"
    file_path.touch()
    mock_datetime.utcnow.side_effect = [
        datetime.datetime(2024, 1, 1, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 2),
        datetime.datetime(2024, 1, 1, 0, 3),
    ]
    job = Job(
        id=1,
        file_loc=create_job_file(file_path, obj),
        start_date=datetime.datetime(2024, 1, 1),
        end_date=datetime.datetime(2024, 1, 1, 0, 2),
        schedule="* * * * *",
    )
    scheduler = Scheduler(mock_executor)
    scheduler.initialize([job])
    scheduler.run()
    assert mock_executor.submit.call_count == 2
