from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from pulse.constants import RuntimeType
from pulse.models import Task, TaskResult, TaskResultStatus
from pulse.runtime import TaskExecutionError


def test_task_queue_send(in_memory_task_queue, mock_executor):
    mock_future = MagicMock(spec=Future)
    mock_executor.submit.side_effect = lambda x: mock_future
    mock_task = MagicMock(spec=Task)
    in_memory_task_queue.send(mock_task)
    mock_executor.submit.assert_called_once_with(mock_task)
    assert in_memory_task_queue.size() == 1
    assert in_memory_task_queue._futures == [mock_future]


@pytest.fixture
def mock_as_completed():
    with patch("pulse.task_queue.as_completed") as mock:
        yield mock


def test_task_queue_receive(mock_as_completed, in_memory_task_queue, mock_executor):
    mock_future1 = MagicMock(spec=Future)
    mock_future1.result.return_value = Task(
        id="1", command="none", runtime=RuntimeType.SUBPROCESS
    )
    mock_future2 = MagicMock(spec=Future)
    mock_future2.result.side_effect = TaskExecutionError(task_id="2")
    mock_as_completed.return_value = iter([mock_future1, mock_future2])
    gen = in_memory_task_queue.receive(timeout=0.1)
    assert list(gen) == [
        TaskResult(id="1", status=TaskResultStatus.SUCCESS),
        TaskResult(id="2", status=TaskResultStatus.FAILED),
    ]


def test_task_queue_receive_timeout(
    mock_as_completed, in_memory_task_queue, mock_executor
):
    mock_as_completed.side_effect = TimeoutError()
    gen = in_memory_task_queue.receive(timeout=0.1)
    assert list(gen) == []
