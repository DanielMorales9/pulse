import datetime
from unittest.mock import patch, create_autospec

import docker
import pytest

from pulse.constants import Runtimes, DEFAULT_DOCKER_IMAGE
from pulse.scheduler import (
    Scheduler,
    Job,
    Runtime,
    SubprocessRuntime,
    DockerRuntime,
)


@pytest.mark.parametrize(
    "job",
    [
        Job(command="echo 'hello world'", runtime=Runtimes.SUBPROCESS),
        Job(command="echo 'hello world'", runtime=Runtimes.DOCKER),
    ],
)
def test_scheduler_run(job):
    mock_runtime = create_autospec(Runtime)
    scheduler = Scheduler()
    scheduler._add(job)
    with patch.object(scheduler, "_get_runtime", return_value=mock_runtime):
        scheduler.run()
        mock_runtime.run.assert_called_once_with(job)


@patch("pulse.scheduler.subprocess")
@pytest.mark.parametrize("command", [["echo", "'hello world'"]])
def test_runtime_subprocess(mock_subprocess, command):
    job = Job(command=command, runtime=Runtimes.SUBPROCESS)
    SubprocessRuntime().run(job)
    mock_subprocess.run.assert_called_once_with(
        command,
        check=True,
        shell=False,
        stdout=mock_subprocess.PIPE,
        stderr=mock_subprocess.PIPE,
        text=True,
    )


@patch("pulse.scheduler.docker")
@pytest.mark.parametrize("command", ["echo 'hello world'"])
def test_runtime_docker(mock_docker, command):
    job = Job(command=command, runtime=Runtimes.DOCKER)
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
    job = Job("echo 'hello world'", Runtimes.SUBPROCESS, schedule)
    assert job.calculate_next_run(at) == expected

