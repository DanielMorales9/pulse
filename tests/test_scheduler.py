import datetime
from concurrent.futures import Future
from unittest.mock import patch

import pytest

from pulse.constants import RuntimeType
from pulse.scheduler import (
    Scheduler,
)
from pulse.models import Job


def _set_result(x):
    future = Future()
    future.set_result(x)
    return future


@pytest.mark.parametrize(
    "job",
    [
        Job(id=0, command="echo 'hello world'", runtime=RuntimeType.SUBPROCESS),
        Job(id=0, command="echo 'hello world'", runtime=RuntimeType.DOCKER),
    ],
)
def test_scheduler_run(job, mock_executor):
    scheduler = Scheduler(mock_executor)
    scheduler.add(job)
    scheduler.run()
    mock_executor.submit.assert_called_once_with(job)


@pytest.mark.parametrize(
    "schedule, expected",
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
def test_job_calculate_next_run(schedule, expected):
    at = datetime.datetime(2020, 1, 1)
    job = Job(0, "echo 'hello world'", RuntimeType.SUBPROCESS, schedule)
    assert job.calculate_next_run(at) == expected


@patch("pulse.scheduler.datetime")
def test_scheduler_loop(mock_datetime, mock_executor):
    mock_datetime.now.side_effect = [
        datetime.datetime(2024, 1, 1, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 2),
        datetime.datetime(2024, 1, 1, 0, 3),
    ]
    scheduler = Scheduler(mock_executor)
    job = Job(
        id=1,
        command="hello world",
        runtime=RuntimeType.SUBPROCESS,
        start_date=datetime.datetime(2024, 1, 1),
        end_date=datetime.datetime(2024, 1, 1, 0, 2),
        schedule="* * * * *",
    )
    scheduler.add(job)
    scheduler.run()
    assert mock_executor.submit.call_count == 2
