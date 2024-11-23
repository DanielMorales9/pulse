import pytest
import yaml
from pathlib import Path

from yaml.scanner import ScannerError

from pulse.utils import save_yaml, load_yaml


@pytest.fixture
def sample_data():
    """Fixture for sample data to save/load."""
    return {"name": "test", "version": 1.0, "features": ["a", "b", "c"]}


def test_save_yaml_creates_file(tmp_path, sample_data):
    # Arrange
    file_path = tmp_path / "test.yaml"

    # Act
    save_yaml(file_path, sample_data)

    # Assert
    assert file_path.exists()
    with open(file_path) as f:
        loaded_content = yaml.safe_load(f)
    assert loaded_content == sample_data


def test_load_yaml_reads_file(tmp_path, sample_data):
    # Arrange
    file_path = tmp_path / "test.yaml"
    with open(file_path, mode="w") as f:
        yaml.safe_dump(sample_data, f)

    # Act
    result = load_yaml(file_path)

    # Assert
    assert result == sample_data


def test_load_yaml_file_not_found():
    # Arrange
    non_existent_file = Path("nonexistent.yaml")

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        load_yaml(non_existent_file)


def test_load_yaml_invalid_format(tmp_path):
    # Arrange
    file_path = tmp_path / "invalid.yaml"
    with open(file_path, mode="w") as f:
        f.write("::: invalid yaml :::")

    # Act & Assert
    with pytest.raises(ScannerError, match="mapping values are not allowed here"):
        load_yaml(file_path)


def test_save_yaml_raises_ioerror_on_invalid_path(sample_data):
    # Arrange
    invalid_file_path = Path("/invalid_path/test.yaml")

    # Act & Assert
    with pytest.raises(IOError):
        save_yaml(invalid_file_path, sample_data)
