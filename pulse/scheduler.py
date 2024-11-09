from concurrent.futures import as_completed, Future
from datetime import datetime

from pulse.executor import SchedulerExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Job


class Scheduler(LoggingMixing):
    TIMEOUT = 1

    def __init__(self, executor: SchedulerExecutor) -> None:
        super().__init__()
        self._jobs: list[Job] = []
        self._executor = executor

    def run(self) -> None:
        futures: list[Future] = []
        while self._jobs or futures:
            at = datetime.now()
            if self._jobs:
                job = self._jobs.pop(0)
                if not job.next_run:
                    start = job.start_date if job.start_date else at
                    job.next_run = job.calculate_next_run(start)

                if at >= job.next_run:
                    future = self._executor.submit(job)
                    futures.append(future)
                else:
                    self._jobs.append(job)

            for future in as_completed(futures, timeout=self.TIMEOUT):
                job = future.result()
                assert job.next_run is not None, "next_run must be non null"
                job.next_run = job.calculate_next_run(job.next_run)

                if not job.completed:
                    self.add(job)
                futures.remove(future)

    def add(self, job: Job) -> None:
        self._jobs.append(job)
