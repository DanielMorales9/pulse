import datetime
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import sessionmaker, session

from pulse.constants import RuntimeType
from pulse.executor import TaskExecutor
from pulse.models import Job, get_cron_next_value, Task, JobRunStatus
from pulse.scheduler import Scheduler, JobRunRepository, JobRepository
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
def mock_session():
    yield MagicMock(spec=session)


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


def test_find_jobs_by_ids(mock_session):
    # Arrange
    ids = ["1", "2", "3"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2"), MagicMock(id="3")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = JobRunRepository.find_jobs_by_ids(ids, mock_session)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.query.return_value.filter.assert_called_once()
    assert result == expected_jobs


def test_find_job_runs_by_state(mock_session):
    # Arrange
    state = JobRunStatus.RUNNING
    expected_jobs = [MagicMock(status=state)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = JobRunRepository.find_job_runs_by_state(state, mock_session)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_find_job_runs_in_states(mock_session):
    # Arrange
    states = [JobRunStatus.RUNNING, JobRunStatus.FAILED]
    expected_jobs = [MagicMock(status=state) for state in states]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = JobRunRepository.find_job_runs_in_states(states, mock_session)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


@patch("pulse.scheduler.JobRunRepository.find_job_runs_in_states")
def test_select_jobs_for_execution(mock_find_job_runs_in_states, mock_session):
    # Arrange
    mock_find_job_runs_in_states.return_value = [
        MagicMock(job_id="1"),
        MagicMock(job_id="2"),
    ]
    limit = 5
    expected_jobs = [MagicMock(next_run="2024-11-02"), MagicMock(next_run="2024-11-03")]
    mock_session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
        expected_jobs
    )

    # Act
    result = JobRepository.select_jobs_for_execution(mock_session, limit)

    # Assert
    mock_find_job_runs_in_states.assert_called_once_with(
        (JobRunStatus.RUNNING, JobRunStatus.FAILED), mock_session
    )
    mock_session.query.assert_called_once()
    assert result == expected_jobs


def test_count_pending_jobs(mock_session):
    # Arrange
    mock_session.query.return_value.filter.return_value.count.return_value = 42

    # Act
    result = JobRepository.count_pending_jobs(mock_session)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == 42


def test_find_jobs_by_ids(mock_session):
    # Arrange
    ids = ["1", "2"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = JobRepository.find_jobs_by_ids(ids, mock_session)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs
