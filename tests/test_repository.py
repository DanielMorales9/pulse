import datetime
from unittest.mock import MagicMock, patch

from pulse.models import Job, JobRun, TaskInstance
from pulse.constants import JobRunStatus, TaskInstanceStatus
from pulse.repository import JobRunRepository


def test_find_job_runs_by_ids(mock_session, job_run_repo):
    # Arrange
    ids = ["1", "2", "3"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2"), MagicMock(id="3")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_run_repo.find_job_runs_by_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.query.return_value.filter.assert_called_once()
    assert result == expected_jobs


def test_find_jobs_by_ids(mock_session, job_repo):
    # Arrange
    ids = ["1", "2"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_repo.find_jobs_by_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_find_jobs_(mock_session, job_repo):
    # Arrange
    ids = ["1", "2"]
    expected_jobs = [MagicMock(id="1"), MagicMock(id="2")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_repo.find_jobs_by_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_calculate_pending_jobs(mock_session, job_repo):
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
    job_repo.calculate_next_run(jobs)

    # Assert
    mock_session.commit.assert_called_once()
    assert jobs[0].next_run == datetime.datetime(year=2024, month=2, day=1)
    assert jobs[0].last_run == datetime.datetime(year=2024, month=1, day=1)
    assert jobs[1].next_run == None
    assert jobs[2].next_run == None
    assert jobs[2].last_run == datetime.datetime(year=2024, month=1, day=1)


def test_find_job_runs_by_state(mock_session, job_run_repo):
    # Arrange
    state = JobRunStatus.RUNNING
    expected_jobs = [MagicMock(status=state)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_run_repo.find_job_runs_by_state(state)

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == expected_jobs


def test_select_jobs_for_execution(mock_session, job_repo):
    # Arrange
    limit = 5
    expected_jobs = [MagicMock(next_run="2024-11-02"), MagicMock(next_run="2024-11-03")]
    mock_session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
        expected_jobs
    )

    # Act
    result = job_repo.get_pending_jobs(limit)

    # Assert
    mock_session.query.assert_called_once()
    assert result == expected_jobs


def test_count_pending_jobs(mock_session, job_repo):
    # Arrange
    mock_session.query.return_value.filter.return_value.count.return_value = 42

    # Act
    result = job_repo.count_pending_jobs()

    # Assert
    mock_session.query.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result == 42


def test_create_run_from_job():
    execution_time = datetime.datetime(year=2024, month=1, day=1)
    mock_job = MagicMock(spec=Job)
    mock_job.id = "1"
    job_run = JobRunRepository.create_job_run_from_job(
        mock_job, execution_time=execution_time
    )
    assert job_run.job_id == "1"
    assert job_run.status == "running"
    assert job_run.execution_time == execution_time


def test_transition_job_runs(mock_session, job_run_repo):
    # Arrange
    expected_jobs = [MagicMock(spec=JobRun, id="1", status=JobRunStatus.RUNNING)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_run_repo.transition_job_runs(["1"], JobRunStatus.FAILED)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.commit.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result[0].id == "1"
    assert result[0].status == JobRunStatus.FAILED


def test_transition_job_runs_by_state(mock_session, job_run_repo):
    # Arrange
    expected_jobs = [MagicMock(spec=JobRun, id="1", status=JobRunStatus.RUNNING)]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = job_run_repo.transition_job_runs_by_state(
        JobRunStatus.FAILED, JobRunStatus.RUNNING
    )

    # Assert
    mock_session.query.assert_called_once()
    mock_session.commit.assert_called_once()
    assert result[0].id == "1"
    assert result[0].status == JobRunStatus.RUNNING


def test_transition_job_runs_state(mock_session, job_run_repo):
    # Arrange
    expected_jobs = [
        MagicMock(spec=JobRun, id="1", status=JobRunStatus.RUNNING, retry_number=0)
    ]

    # Act
    result = job_run_repo.transition_job_runs_state(expected_jobs, JobRunStatus.FAILED)

    # Assert
    mock_session.commit.assert_called_once()
    assert result[0].id == "1"
    assert result[0].status == JobRunStatus.FAILED
    assert result[0].retry_number == 1


def test_transition_task_instances_state(mock_session, ti_repo):
    # Arrange
    expected_tis = [
        MagicMock(
            spec=TaskInstance, id="1", status=TaskInstanceStatus.RUNNING, retry_number=0
        )
    ]

    # Act
    result = ti_repo.transition_task_instances_state(
        expected_tis, TaskInstanceStatus.FAILED
    )

    # Assert
    mock_session.commit.assert_called_once()
    assert result[0].id == "1"
    assert result[0].status == TaskInstanceStatus.FAILED
    assert result[0].retry_number == 1


def test_transition_task_instances(mock_session, ti_repo):
    # Arrange
    expected_jobs = [
        MagicMock(spec=TaskInstance, id="1", status=TaskInstanceStatus.RUNNING)
    ]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_jobs

    # Act
    result = ti_repo.transition_task_instances(["1"], TaskInstanceStatus.FAILED)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.commit.assert_called_once()
    assert mock_session.query.return_value.filter.called
    assert result[0].id == "1"
    assert result[0].status == TaskInstanceStatus.FAILED


def test_find_task_instances_by_ids(mock_session, ti_repo):
    # Arrange
    ids = ["1", "2", "3"]
    expected_tis = [MagicMock(id="1"), MagicMock(id="2"), MagicMock(id="3")]
    mock_session.query.return_value.filter.return_value.all.return_value = expected_tis

    # Act
    result = ti_repo.find_task_instances_by_job_run_ids(ids)

    # Assert
    mock_session.query.assert_called_once()
    mock_session.query.return_value.filter.assert_called_once()
    assert result == expected_tis


@patch("pulse.repository.load_yaml")
def test_create_task_instances_from_job_runs(mock_load_yaml, mock_session, ti_repo):
    mock_load_yaml.return_value = {
        "runtime": "subprocess",
        "command": "echo 'hello world'",
    }
    mock_job = MagicMock(spec=Job, file_loc="/file/loc/config.yaml")
    job_runs = [
        MagicMock(spec=JobRun, id="1", job=mock_job),
        MagicMock(spec=JobRun, id="2", job=mock_job),
    ]

    tis = ti_repo.create_task_instances_from_job_runs(job_runs)
    mock_session.add_all.assert_called_once()
    mock_session.commit.assert_called_once()

    assert tis[0].status == "running"
    assert tis[0].job_run_id == "1"
    assert tis[0].runtime == "subprocess"
    assert tis[0].command == "echo 'hello world'"
    assert tis[1].status == "running"
    assert tis[1].job_run_id == "2"
    assert tis[1].runtime == "subprocess"
    assert tis[1].command == "echo 'hello world'"


@patch("pulse.repository.load_yaml")
def test_create_task_instance_from_job_run(mock_load_yaml, ti_repo):
    mock_load_yaml.return_value = {
        "runtime": "subprocess",
        "command": "echo 'hello world'",
    }
    mock_job = MagicMock(spec=Job, file_loc="/file/loc/config.yaml")

    mock_job_run = MagicMock(spec=JobRun, id="1", job=mock_job)
    ti = ti_repo.create_task_instance_from_job_run(mock_job_run)
    assert ti.status == "running"
    assert ti.job_run_id == "1"
    assert ti.command == "echo 'hello world'"
    assert ti.runtime == "subprocess"


@patch("pulse.repository.datetime")
def test_create_job_runs_from_job(mock_datetime, mock_session, job_run_repo):
    at = datetime.datetime(2023, 1, 1)
    mock_datetime.utcnow.return_value = at
    mock_job_run = MagicMock(
        spec=Job,
        id="1",
    )
    job_runs = job_run_repo.create_job_runs_from_jobs([mock_job_run])
    mock_session.add_all.assert_called_once()
    mock_session.commit.assert_called_once()
    assert len(job_runs) == 1
    assert job_runs[0].job_id == "1"
    assert job_runs[0].execution_time == at
    assert job_runs[0].status == "running"
