from unittest.mock import create_autospec

import pytest

from pulse.executor import SchedulerExecutor
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
    mock_executor = create_autospec(SchedulerExecutor)
    mock_executor.submit.side_effect = _set_result
    yield mock_executor
