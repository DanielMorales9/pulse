from unittest.mock import patch


from pulse.scheduler import Scheduler, Job


@patch("pulse.scheduler.subprocess")
def test_scheduler_run(mock_subprocess):
    job = Job("echo 'hello world'")
    scheduler = Scheduler()
    scheduler.add(job)
    scheduler.run()
    assert mock_subprocess.run.called
