from concurrent.futures import Future
from unittest.mock import create_autospec, MagicMock

import pytest
from sqlalchemy.orm import Session, sessionmaker

from pulse.executor import TaskExecutor
from pulse.repository import JobRepository, JobRunRepository, TaskInstanceRepository
from pulse.runtime import Runtime, RuntimeManager
from pulse.scheduler import Scheduler
from pulse.task_queue import TaskQueue, InMemoryTaskQueue


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


def _set_result(x):
    future = Future()
    future.set_result(x)
    return future


@pytest.fixture
def mock_create_session(mock_session):
    mock_create_session = MagicMock(spec=sessionmaker)
    mock_create_session.return_value.__enter__.return_value = mock_session
    yield mock_create_session


@pytest.fixture
def mock_task_queue():
    yield MagicMock(spec=TaskQueue)


@pytest.fixture
def in_memory_task_queue(mock_executor):
    yield InMemoryTaskQueue(mock_executor)


@pytest.fixture
def scheduler(mock_executor, mock_create_session, mock_task_queue):
    yield Scheduler(mock_create_session, task_queue=mock_task_queue)
