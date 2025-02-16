from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from pulse.constants import RuntimeType
from pulse.models import Job, Task, JobRun, TaskInstance
from pulse.repository import JobRepository, JobRunRepository
from pulse.scheduler import (
    RuntimeInconsistencyCheckError,
    check_for_inconsistent_task_instances,
)
from pulse.utils import save_yaml


@pytest.fixture
def mock_job(request, tmp_path):
    obj = request.param
    file_path = tmp_path / "job.yaml"
    file_path.touch()
    save_yaml(file_path, obj)
    yield Job(file_loc=str(file_path))


def test_execute_tasks(scheduler, mock_task_queue):
    mock_task = MagicMock(spec=TaskInstance)
    mock_task.exchange_data = mock_exchange = Task(
        id="job1",
        command="command",
        runtime=RuntimeType.SUBPROCESS,
    )
    scheduler.execute_tasks([mock_task])
    mock_task_queue.send.assert_called_once_with(mock_exchange)


# Parametrize test for valid cases (no duplicates)
@pytest.mark.parametrize(
    "result",
    [
        dict(
            success=["job_1", "job_2", "job_3"],
            failed=["job_4", "job_5"],
        ),
        dict(
            success=["job_1", "job_2"],
            failed=["job_3", "job_4"],
        ),
    ],
)
def test_no_inconsistencies(result):
    # Should not raise an exception
    try:
        check_for_inconsistent_task_instances(**result)
    except RuntimeInconsistencyCheckError:
        pytest.fail("RuntimeInconsistencyCheckError raised unexpectedly!")


# Parametrize test for duplicate job IDs in success or failed statuses
@pytest.mark.parametrize(
    "result, expected_message",
    [
        (
            dict(
                success=[
                    "job_1",
                    "job_2",
                    "job_2",
                ],  # Duplicate job_2 in success
                failed=["job_3", "job_4"],
            ),
            "Duplicate Task Instance IDs found in status 'success'",
        ),
        (
            dict(
                success=["job_1", "job_2", "job_3"],
                failed=[
                    "job_2",
                    "job_2",
                    "job_4",
                ],  # Duplicate job_2 in failed
            ),
            "Duplicate Task Instance IDs found in status 'failed'",
        ),
        (
            dict(
                success=["job_1", "job_2", "job_3"],
                failed=[
                    "job_2",
                    "job_4",
                ],  # job_2 is in both success and failed
            ),
            "Duplicate Task Instance IDs found more than one status",
        ),
    ],
)
def test_inconsistencies(result, expected_message: str):
    # Should raise RuntimeInconsistencyCheckError with the correct message
    with pytest.raises(RuntimeInconsistencyCheckError, match=expected_message):
        check_for_inconsistent_task_instances(**result)


@pytest.fixture
def running_jobs():
    yield [MagicMock(), MagicMock()]


def test_cm_session(scheduler, mock_create_session, mock_session):
    mock_create_session.return_value = mock_session
    with scheduler:
        assert isinstance(scheduler._session, Session)
        assert isinstance(scheduler._job_repo, JobRepository)
        assert isinstance(scheduler._job_run_repo, JobRunRepository)

    mock_session.close.assert_called_once()


@patch("pulse.repository.datetime")
def test_create_runs_for_pending_jobs(
    mock_datetime, scheduler, mock_session, mock_job_repo, mock_job_run_repo
):
    at = datetime(2023, 1, 1)
    mock_datetime.utcnow.return_value = at
    scheduler._session = mock_session
    scheduler._job_repo = mock_job_repo
    scheduler._job_run_repo = mock_job_run_repo

    # Mock repository methods
    mock_jobs = [
        MagicMock(spec=Job, id="1"),
        MagicMock(spec=Job, id="2"),
    ]
    mock_job_repo.get_pending_jobs.return_value = mock_jobs

    # Mock create_run_from_job to return JobRun instances
    mock_job_run_repo.create_job_runs_from_jobs.return_value = [
        MagicMock(spec=JobRun, id="1", execution_time=at),
        MagicMock(spec=JobRun, id="2", execution_time=at),
    ]

    # Call the method under test
    job_runs = scheduler.create_pending_job_runs()

    # Assertions
    assert len(job_runs) == len(mock_jobs)
    for job_run, job in zip(job_runs, mock_jobs):
        assert job_run.id == job.id
        assert isinstance(job_run.execution_time, datetime)

    # Verify interactions with mocked dependencies
    mock_job_repo.get_pending_jobs.assert_called_once_with(scheduler.MAX_RUN_PER_CYCLE)
    mock_job_run_repo.create_job_runs_from_jobs.assert_any_call(mock_jobs)
