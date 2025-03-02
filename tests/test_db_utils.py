import pytest
from sqlalchemy import inspect

from pulse.db_utils import init_db, Base


@pytest.fixture
def temp_db(tmp_path):
    """Creates a temporary database file for testing."""
    return tmp_path / "test_db.sqlite"


def test_init_db_creates_db(temp_db):
    """Test that init_db creates a database and tables."""
    session_factory = init_db(temp_db)

    assert temp_db.exists(), "Database file was not created"

    session = session_factory()
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()

    assert set(Base.metadata.tables.keys()).issubset(
        tables
    ), "Not all tables were created"


def test_init_db_drops_tables(temp_db):
    """Test that init_db drops tables when drop=True."""
    # Initialize DB and create tables
    session_factory = init_db(temp_db)
    session = session_factory()
    inspector = inspect(session.bind)

    assert inspector.get_table_names(), "Tables should exist after creation"

    # Reinitialize DB with drop=True
    init_db(temp_db, drop=True)
    inspector = inspect(session.bind)

    assert inspector.get_table_names() == [
        "job_runs",
        "jobs",
        "task_instances",
    ], "Tables were not dropped"
