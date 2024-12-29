import datetime
from unittest.mock import MagicMock, patch

import pytest

from pulse.models import Job, JobRun
from pulse.constants import JobRunStatus
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


def test_find_jobs_(mock_session, mock_job_repo):
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


def test_calculate_pending_jobs(mock_session, mock_job_repo):
    # Arrange
    jobs = [
        MagicMock(
            spec=Job,
            id="1",
            schedule="0 0 1 * *",
            next_run=datetime.datetime(year=2024, month=1, day=1),
            end_date=None,
        ),
        MagicMock(spec=Job, id="2", schedule=None, next_run=None, end_date=None),
        MagicMock(
            spec=Job,
            id="3",
            schedule=None,
            next_run=datetime.datetime(year=2024, month=1, day=1),
            last_run=None,
        ),
    ]

    # Act
    mock_job_repo.calculate_next_run(jobs)

    # Assert
    mock_session.commit.assert_called_once()
    assert jobs[0].next_run == datetime.datetime(year=2024, month=2, day=1)
    assert jobs[0].last_run == datetime.datetime(year=2024, month=1, day=1)
    assert jobs[1].next_run == None
    assert jobs[2].next_run == None
    assert jobs[2].last_run == datetime.datetime(year=2024, month=1, day=1)


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


def test_select_jobs_for_execution(mock_session, mock_job_repo):
    # Arrange
    limit = 5
    expected_jobs = [MagicMock(next_run="2024-11-02"), MagicMock(next_run="2024-11-03")]
    mock_session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
        expected_jobs
    )

    # Act
    result = mock_job_repo.get_pending_jobs(limit)

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


def test_transition_job_runs(mock_session, mock_job_run_repo):
    # Arrange
    expected_jobs = [MagicMock(spec=JobRun, id="1", status=JobRunStatus.RUNNING)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_run_repo.transition_job_runs(["1"], JobRunStatus.FAILED)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.commit.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result[0].id == "1"
    assert result[0].status == JobRunStatus.FAILED


def test_transition_job_runs_by_state(mock_session, mock_job_run_repo):
    # Arrange
    expected_jobs = [MagicMock(spec=JobRun, id="1", status=JobRunStatus.RUNNING)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = mock_job_run_repo.transition_job_runs_by_state(
        JobRunStatus.FAILED, JobRunStatus.RUNNING
    )

    # Assert
    mock_session.query.assert_called_once()
    mock_session.commit.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result[0].id == "1"
    assert result[0].status == JobRunStatus.RUNNING
