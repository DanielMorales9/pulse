from datetime import datetime

from pulse.models import Job
from pulse.runtime import RuntimeManager


class Scheduler:
    def __init__(self, runtime_manager: RuntimeManager) -> None:
        self._jobs: list[Job] = []
        self._runtime_manager = runtime_manager

    def _execute(self, job: Job) -> None:
        runtime = self._runtime_manager.get_runtime(job.runtime)
        runtime.run(job)

    def run(self) -> None:
        while self._jobs:
            at = datetime.now()
            job = self._jobs.pop(0)
            if not job.next_run:
                job.next_run = job.calculate_next_run(at)

            if at >= job.next_run:
                self._execute(job)
                job.next_run = job.calculate_next_run(job.next_run)

            if not job.completed:
                self.add(job)

    def add(self, job: Job) -> None:
        self._jobs.append(job)
