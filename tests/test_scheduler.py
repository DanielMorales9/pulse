from unittest.mock import patch


from scheduler.scheduler import Scheduler, Job


@patch("scheduler.scheduler.subprocess")
def test_scheduler_run(mock_subprocess):
    job = Job("echo 'hello world'")
    scheduler = Scheduler()
    scheduler.add(job)
    scheduler.run()
    assert mock_subprocess.run.called
