from unittest.mock import patch, create_autospec

import docker
import pytest

from pulse.constants import RuntimeType, DEFAULT_DOCKER_IMAGE
from pulse.models import Job
from pulse.runtime import SubprocessRuntime, DockerRuntime


@patch("pulse.runtime.subprocess")
@pytest.mark.parametrize("command", [["echo", "'hello world'"]])
def test_runtime_subprocess(mock_subprocess, command):
    job = Job(id=0, command=command, runtime=RuntimeType.SUBPROCESS)
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
    job = Job(id=0, command=command, runtime=RuntimeType.DOCKER)
    mock_docker.from_env.return_value = mock_docker_client = create_autospec(
        docker.DockerClient
    )
    DockerRuntime().run(job)
    mock_docker_client.containers.run.assert_called_once_with(
        DEFAULT_DOCKER_IMAGE, command, detach=True
    )
