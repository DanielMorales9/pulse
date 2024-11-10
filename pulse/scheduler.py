from concurrent.futures import as_completed, Future
from datetime import datetime

from pulse.constants import DEFAULT_MAX_PARALLELISM
from pulse.executor import JobExecutor
from pulse.logutils import LoggingMixing
from pulse.models import Job


class Scheduler(LoggingMixing):
    TIMEOUT = 0.1
    MAX_RUN_PER_CYCLE = 10

    def __init__(
        self, executor: JobExecutor, max_parallelism: int = DEFAULT_MAX_PARALLELISM
    ) -> None:
        super().__init__()
        self._pending_jobs: dict[int, Job] = {}
        self._running_jobs: list[Future] = []
        self._max_parallelism = max_parallelism
        self._executor = executor

    def initialize(self, jobs: list[Job]) -> None:
        at = datetime.now()
        for job in jobs:
            if not job.next_run:
                start = job.start_date if job.start_date else at
                job.next_run = job.calculate_next_run(start)
            self._pending_jobs[job.id] = job

    @staticmethod
    def job_priority(job: Job) -> float:
        assert job.next_run
        return job.next_run.timestamp()

    def _select_jobs_for_execution(self, at: datetime) -> list[Job]:
        due_jobs = (
            job
            for job in self._pending_jobs.values()
            if job.next_run and at >= job.next_run
        )
        sorted_jobs = sorted(due_jobs, key=self.job_priority)
        return sorted_jobs[: self.MAX_RUN_PER_CYCLE]

    def run(self) -> None:
        while self._pending_jobs or self._running_jobs:
            at = datetime.now()
            if len(self._running_jobs) < self._max_parallelism:
                scheduled = self._schedule_pending(at)
                self._running_jobs.extend(scheduled)
            completed_jobs = self._wait_for_completion()
            self._calculate_pending(completed_jobs)

    def _calculate_pending(self, jobs: list[Job]) -> None:
        for job in jobs:
            assert job.next_run
            job.next_run = job.calculate_next_run(job.next_run)
            if not job.completed:
                self._pending_jobs[job.id] = job

    def _wait_for_completion(self) -> list[Job]:
        completed_jobs = []
        try:
            for future in as_completed(self._running_jobs, timeout=self.TIMEOUT):
                job = future.result()
                completed_jobs.append(job)
                self._running_jobs.remove(future)
        except TimeoutError:
            self.logger.info("Timeout exceeded")
        finally:
            return completed_jobs

    def _schedule_pending(self, at: datetime) -> list[Future]:
        result = []
        for job in self._select_jobs_for_execution(at):
            job.execution_time = at
            future = self._executor.submit(job)
            del self._pending_jobs[job.id]
            result.append(future)
        return result
