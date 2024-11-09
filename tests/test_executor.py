from unittest.mock import patch, MagicMock

from pulse.executor import ThreadJobExecutor, _execute, ProcessJobExecutor
from pulse.models import Job


@patch("pulse.executor.ThreadPoolExecutor")
def test_thread_executor(mock_pool_executor_class):
    mock_pool_executor = mock_pool_executor_class.return_value
    executor = ThreadJobExecutor()
    magic_mock = MagicMock(spec=Job)
    executor.submit(magic_mock)
    mock_pool_executor.submit.assert_called_once_with(_execute, magic_mock)


@patch("pulse.executor.ProcessPoolExecutor")
def test_process_executor(mock_pool_executor_class):
    mock_pool_executor = mock_pool_executor_class.return_value
    executor = ProcessJobExecutor()
    magic_mock = MagicMock(spec=Job)
    executor.submit(magic_mock)
    mock_pool_executor.submit.assert_called_once_with(_execute, magic_mock)
