import datetime
from unittest.mock import patch, create_autospec

import docker
import pytest

from pulse.constants import RuntimeType, DEFAULT_DOCKER_IMAGE
from pulse.scheduler import (
    Scheduler,
)
from pulse.models import Job
from pulse.runtime import Runtime, SubprocessRuntime, DockerRuntime, RuntimeManager


@pytest.mark.parametrize(
    "job",
    [
        Job(command="echo 'hello world'", runtime=RuntimeType.SUBPROCESS),
        Job(command="echo 'hello world'", runtime=RuntimeType.DOCKER),
    ],
)
def test_scheduler_run(job):
    mock_runtime = create_autospec(Runtime)
    mock_runtime_mgr = create_autospec(RuntimeManager)
    mock_runtime_mgr.get_runtime.return_value = mock_runtime
    scheduler = Scheduler(mock_runtime_mgr)
    scheduler._add(job)
    scheduler.run()
    mock_runtime.run.assert_called_once_with(job)


@patch("pulse.runtime.subprocess")
@pytest.mark.parametrize("command", [["echo", "'hello world'"]])
def test_runtime_subprocess(mock_subprocess, command):
    job = Job(command=command, runtime=RuntimeType.SUBPROCESS)
    SubprocessRuntime().run(job)
    mock_subprocess.run.assert_called_once_with(
        command,
        check=True,
        shell=False,
        stdout=mock_subprocess.PIPE,
        stderr=mock_subprocess.PIPE,
        text=True,
    )


@patch("pulse.runtime.docker")
@pytest.mark.parametrize("command", ["echo 'hello world'"])
def test_runtime_docker(mock_docker, command):
    job = Job(command=command, runtime=RuntimeType.DOCKER)
    mock_docker.from_env.return_value = mock_docker_client = create_autospec(
        docker.DockerClient
    )
    DockerRuntime().run(job)
    mock_docker_client.containers.run.assert_called_once_with(
        DEFAULT_DOCKER_IMAGE, command, detach=True
    )


@pytest.mark.parametrize(
    "schedule, expected",
    [
        (
            "* * * * *",
            datetime.datetime(2020, 1, 1, 0, 1),
        ),
        (
            "0 * * * *",
            datetime.datetime(2020, 1, 1, 1, 0),
        ),
    ],
)
def test_job_calculate_next_run(schedule, expected):
    at = datetime.datetime(2020, 1, 1)
    job = Job("echo 'hello world'", RuntimeType.SUBPROCESS, schedule)
    assert job.calculate_next_run(at) == expected
