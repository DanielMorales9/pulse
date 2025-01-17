from unittest.mock import patch, MagicMock

import pytest

from pulse.constants import JobExecutorType
from pulse.executor import (
    ThreadTaskExecutor,
    _execute,
    ProcessTaskExecutor,
    JobExecutorManager,
)
from pulse.models import Task


@patch("pulse.executor.ThreadPoolExecutor")
def test_thread_executor(mock_pool_executor_class):
    mock_pool_executor = mock_pool_executor_class.return_value
    executor = ThreadTaskExecutor()
    magic_mock = MagicMock(spec=Task)
    executor.submit(magic_mock)
    mock_pool_executor.submit.assert_called_once_with(_execute, magic_mock)


@patch("pulse.executor.ProcessPoolExecutor")
def test_process_executor(mock_pool_executor_class):
    mock_pool_executor = mock_pool_executor_class.return_value
    executor = ProcessTaskExecutor()
    magic_mock = MagicMock(spec=Task)
    executor.submit(magic_mock)
    mock_pool_executor.submit.assert_called_once_with(_execute, magic_mock)


@pytest.fixture
def manager():
    return JobExecutorManager()


def test_get_executor_thread(manager):
    # Test if THREAD executor type returns an instance of ThreadTaskExecutor
    executor = manager.get_executor(JobExecutorType.THREAD)
    assert isinstance(executor, ThreadTaskExecutor)


def test_get_executor_process(manager):
    # Test if PROCESS executor type returns an instance of ProcessTaskExecutor
    executor = manager.get_executor(JobExecutorType.PROCESS)
    assert isinstance(executor, ProcessTaskExecutor)


@patch("os.cpu_count", return_value=4)
def test_parallelism_thread(mock_cpu_count):
    # Test parallelism for THREAD executor type
    parallelism = JobExecutorManager.parallelism(JobExecutorType.THREAD)
    assert parallelism == 8  # 4 (cpu count) + 4


@patch("os.cpu_count", return_value=4)
def test_parallelism_process(mock_cpu_count):
    # Test parallelism for PROCESS executor type
    parallelism = JobExecutorManager.parallelism(JobExecutorType.PROCESS)
    assert parallelism == 4  # Only uses cpu count without additional offset


@patch("os.cpu_count", return_value=None)  # Simulate cpu_count returning None
def test_parallelism_no_cpu_count(mock_cpu_count):
    # Test parallelism when cpu_count returns None
    parallelism = JobExecutorManager.parallelism(JobExecutorType.THREAD)
    assert parallelism == 5  # Defaults to 1 + 4
