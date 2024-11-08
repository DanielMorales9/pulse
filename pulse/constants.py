from enum import StrEnum

DEFAULT_DOCKER_IMAGE = "ubuntu"


class Runtimes(StrEnum):
    SUBPROCESS = "subprocess"
    DOCKER = "docker"
