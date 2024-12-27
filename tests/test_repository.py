import datetime
from unittest.mock import MagicMock, patch

import pytest

from pulse.models import JobRunStatus, Job, JobRun
from pulse.repository import JobRunRepository, JobRepository


@pytest.fixture
def mock_job_repo(mock_session):
    yield JobRepository(mock_session)


@pytest.fixture
def mock_job_run_repo(mock_session):
    yield JobRunRepository(mock_session)


def test_find_job_runs_by_ids(mock_session, mock_job_run_repo):
    # Arrange
    ids = ["1", "2", "3"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2"), MagicMock(id="3")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_run_repo.find_job_runs_by_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.query.return_value.filter.assert_called_once()
    assert result == expected_jobs


def test_find_jobs_by_ids(mock_session, mock_job_repo):
    # Arrange
    ids = ["1", "2"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_repo.find_jobs_by_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_find_job_runs_by_state(mock_session, mock_job_run_repo):
    # Arrange
    state = JobRunStatus.RUNNING
    expected_jobs = [MagicMock(status=state)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_run_repo.find_job_runs_by_state(state)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_find_job_runs_in_states(mock_session, mock_job_run_repo):
    # Arrange
    states = [JobRunStatus.RUNNING, JobRunStatus.FAILED]
    expected_jobs = [MagicMock(status=state) for state in states]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_run_repo.find_job_runs_in_states(states)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_select_jobs_for_execution(mock_session, mock_job_repo):
    # Arrange
    limit = 5
    expected_jobs = [MagicMock(next_run="2024-11-02"), MagicMock(next_run="2024-11-03")]
    mock_session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
        expected_jobs
    )

    # Act
    result = mock_job_repo.select_jobs_for_execution(["1", "2"], limit)

    # Assert
    mock_session.query.assert_called_once()
    assert result == expected_jobs


def test_count_pending_jobs(mock_session, mock_job_repo):
    # Arrange
    mock_session.query.return_value.filter.return_value.count.return_value = 42

    # Act
    result = mock_job_repo.count_pending_jobs()

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == 42


def test_create_run_from_job():
    execution_time = datetime.datetime(year=2024, month=1, day=1)
    mock_job = MagicMock(spec=Job)
    mock_job.id = "1"
    job_run = JobRunRepository.create_run_from_job(
        mock_job, execution_time=execution_time
    )
    assert job_run.job_id == "1"
    assert job_run.status == "running"
    assert job_run.retry_number == 0
    assert job_run.execution_time == execution_time
