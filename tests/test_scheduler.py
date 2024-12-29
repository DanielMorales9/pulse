from concurrent.futures import Future
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import sessionmaker, Session

from pulse.constants import RuntimeType
from pulse.executor import TaskExecutor
from pulse.models import Job, Task, JobRun, TaskInstance
from pulse.repository import JobRepository, JobRunRepository
from pulse.runtime import TaskExecutionError
from pulse.scheduler import (
    Scheduler,
    RuntimeInconsistencyCheckError,
    check_for_inconsistent_task_instances,
    TasksResults,
)
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
def scheduler(mock_executor, mock_create_session):
    yield Scheduler(mock_executor, mock_create_session)


def test_execute_tasks(scheduler, mock_executor):
    mock_task = MagicMock(spec=TaskInstance)
    mock_task.exchange_data = mock_exchange = Task(
        id="job1",
        command="command",
        runtime=RuntimeType.SUBPROCESS,
    )
    scheduler.execute_tasks([mock_task])
    mock_executor.submit.assert_called_once_with(mock_exchange)


# Parametrize test for valid cases (no duplicates)
@pytest.mark.parametrize(
    "result",
    [
        TasksResults(
            success=["job_1", "job_2", "job_3"],
            failed=["job_4", "job_5"],
        ),
        TasksResults(
            success=["job_1", "job_2"],
            failed=["job_3", "job_4"],
        ),
    ],
)
def test_no_inconsistencies(result):
    # Should not raise an exception
    try:
        check_for_inconsistent_task_instances(result)
    except RuntimeInconsistencyCheckError:
        pytest.fail("RuntimeInconsistencyCheckError raised unexpectedly!")


# Parametrize test for duplicate job IDs in success or failed statuses
@pytest.mark.parametrize(
    "result, expected_message",
    [
        (
            TasksResults(
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
            TasksResults(
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
            TasksResults(
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
        check_for_inconsistent_task_instances(result)


@pytest.fixture
def running_jobs():
    yield [MagicMock(), MagicMock()]


@patch("pulse.scheduler.as_completed")
def test_wait_for_completion_success(mock_as_completed, scheduler, running_jobs):
    # Mock the _futures and result behavior
    job_task = MagicMock(spec=TaskInstance, job_run_id="job_run_1", id="task_1")
    running_jobs[0].result.return_value = job_task  # Mock result for successful task
    running_jobs[1].result.return_value = job_task  # Mock result for successful task
    mock_as_completed.return_value = running_jobs

    result = scheduler.wait_for_completion()

    # Check that the result dictionary has the correct success status
    assert result.success == ["task_1", "task_1"]
    # Ensure that the future result method was called
    running_jobs[0].result.assert_called_once()
    running_jobs[1].result.assert_called_once()


@patch("pulse.scheduler.as_completed")
def test_wait_for_completion_failure(mock_as_completed, scheduler, running_jobs):
    # Mock the _futures and result behavior to raise TaskExecutionError
    running_jobs[0].result.side_effect = TaskExecutionError("task_1")
    running_jobs[1].result.side_effect = TaskExecutionError("task_2")
    mock_as_completed.return_value = running_jobs

    result = scheduler.wait_for_completion()

    # Check that the result dictionary has the correct failure status
    assert result.failed == ["task_1", "task_2"]
    # Ensure that the future result method was called
    running_jobs[0].result.assert_called_once()
    running_jobs[1].result.assert_called_once()


@patch("pulse.scheduler.as_completed")
def test_wait_for_completion_timeout(mock_as_completed, scheduler, running_jobs):
    # Simulate timeout by making the future jobs take too long
    running_jobs[0].result.side_effect = TimeoutError("job_1 timeout")
    running_jobs[1].result.side_effect = TimeoutError("job_2 timeout")
    mock_as_completed.return_value = running_jobs

    result = scheduler.wait_for_completion()

    # Test that the result is empty or whatever you expect in case of timeout
    assert result == TasksResults()


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
