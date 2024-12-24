import datetime
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import sessionmaker

from pulse.constants import RuntimeType
from pulse.executor import TaskExecutor
from pulse.models import Job, get_cron_next_value, Task
from pulse.scheduler import Scheduler
from pulse.utils import save_yaml


def _set_result(x):
    future = Future()
    future.set_result(x)
    return future


@pytest.fixture
def mock_job(request, tmp_path):
    obj = request.param
    file_path = tmp_path / "job.yaml"
    file_path.touch()
    save_yaml(file_path, obj)
    yield Job(file_loc=str(file_path))


@pytest.fixture
def mock_create_session(mock_session):
    mock_create_session = MagicMock(spec=sessionmaker)
    mock_create_session.return_value.__enter__.return_value = mock_session
    yield mock_create_session


@pytest.fixture
def mock_executor():
    yield MagicMock(spec=TaskExecutor)


@pytest.fixture
def scheduler_fixture(mock_executor, mock_create_session):
    yield Scheduler(mock_executor, mock_create_session)


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


def test_execute_tasks(scheduler_fixture, mock_executor):
    task = Task(
        job_id="job1",
        job_run_id="run1",
        command="command",
        runtime=RuntimeType.SUBPROCESS,
    )
    tasks = [task]
    scheduler_fixture._execute_tasks(tasks)
    mock_executor.submit.assert_called_once_with(task)
