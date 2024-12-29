from unittest.mock import create_autospec

import pytest
from sqlalchemy.orm import Session

from pulse.executor import TaskExecutor
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
