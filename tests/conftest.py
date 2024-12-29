from unittest.mock import create_autospec

import pytest
from sqlalchemy.orm import Session

from pulse.executor import TaskExecutor
from pulse.repository import JobRepository, JobRunRepository, TaskInstanceRepository
from pulse.runtime import Runtime, RuntimeManager
from tests.test_scheduler import _set_result


@pytest.fixture
def mock_runtime():
    yield create_autospec(Runtime)


@pytest.fixture
def mock_runtime_mgr(mock_runtime):
    mock_mgr = create_autospec(RuntimeManager)
    mock_mgr.get_runtime.return_value = mock_runtime
    yield mock_mgr


@pytest.fixture
def mock_executor():
    mock_executor = create_autospec(TaskExecutor)
    mock_executor.submit.side_effect = _set_result
    yield mock_executor


@pytest.fixture
def mock_session():
    yield create_autospec(Session)


@pytest.fixture
def job_repo(mock_session):
    yield JobRepository(mock_session)


@pytest.fixture
def job_run_repo(mock_session):
    yield JobRunRepository(mock_session)


@pytest.fixture
def ti_repo(mock_session):
    yield TaskInstanceRepository(mock_session)


@pytest.fixture
def mock_job_repo(mock_session):
    yield create_autospec(JobRepository)


@pytest.fixture
def mock_job_run_repo(mock_session):
    yield create_autospec(JobRunRepository)


@pytest.fixture
def mock_ti_repo(mock_session):
    yield create_autospec(TaskInstanceRepository)
