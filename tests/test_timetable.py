from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from pulse.models import Job
from pulse.timetable import CronTimetable, OnceTimetable, create_timetable


@pytest.fixture
def mock_job():
    return MagicMock(spec=Job)


@patch("pulse.timetable.get_cron_next_value")
def test_cron_timetable_calculate(mock_cron_next_value, mock_job):
    # Setup mock values for the job
    mock_job.schedule = "0 0 1 * *"  # A cron schedule (example: run at midnight on the first day of the month)
    mock_job.start_date = datetime(2024, 1, 1)
    mock_job.last_run = datetime(2023, 12, 1)
    mock_job.next_run = datetime(2024, 1, 1)
    mock_job.end_date = None  # No end date for the test

    mock_cron_next_value.return_value = datetime(2024, 2, 1)

    timetable = CronTimetable()
    next_run = timetable.calculate(mock_job)

    mock_cron_next_value.assert_called_once_with(mock_job.schedule, mock_job.last_run)
    assert next_run == datetime(2024, 2, 1)


def test_once_timetable_calculate(mock_job):
    # Case 1: Job has a last_run, so it should return None
    mock_job.last_run = datetime(2024, 1, 1)
    timetable = OnceTimetable()
    next_run = timetable.calculate(mock_job)
    assert next_run is None

    # Case 2: Job has no last_run, so it should return the start_date
    mock_job.last_run = None
    mock_job.start_date = datetime(2024, 2, 1)
    next_run = timetable.calculate(mock_job)
    assert next_run == datetime(2024, 2, 1)


def test_create_timetable_with_schedule():
    timetable = create_timetable("0 0 1 * *")
    assert isinstance(timetable, CronTimetable)


def test_create_timetable_without_schedule():
    timetable = create_timetable(None)
    assert isinstance(timetable, OnceTimetable)
