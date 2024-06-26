"""Tests for the download_gbif module."""

import zipfile
from pathlib import Path

import pytest
from pygbif import occurrences as occ

from src.gbif.get_gbif_data import (
    GbifDownloadFailure,
    check_download_job_and_download_file,
    check_download_status,
    download_request_to_disk,
    init_gbif_download,
    unzip_and_rename,
)

# Define the module name to be used for mocking
TESTED_MODULE = "src.gbif.get_gbif_data"


@pytest.fixture(name="gbif_query")
def fixture_gbif_query() -> dict:
    """Sample GBIF query"""
    return {
        "type": "and",
        "predicates": [
            {
                "type": "equals",
                "key": "TAXON_KEY",
                "value": "2653316",  # Ephedra equisetina, only one record
            },
            {"type": "not", "key": "DEGREE_OF_ESTABLISHMENT", "value": "cultivated"},
        ],
    }


@pytest.fixture(name="gbif_download_info")
def fixture_gbif_download_info() -> tuple:
    """Sample GBIF download info"""
    return (
        "0000000-000000000000000",
        {
            "creator": "lusk",
            "notification_address": ["daniel.lusk@geosense.uni-freiburg.de"],
            "sendNotification": True,
            "predicate": {
                "type": "and",
                "predicates": [
                    {"type": "equals", "key": "TAXON_KEY", "value": "2653316"},
                    {
                        "type": "not",
                        "predicate": {
                            "type": "in",
                            "key": "DEGREE_OF_ESTABLISHMENT",
                            "values": ["cultivated"],
                        },
                    },
                ],
            },
            "format": "SIMPLE_CSV",
        },
    )


@pytest.fixture(name="gbif_download_meta_running")
def fixture_gbif_download_meta_running() -> dict:
    """Sample GBIF download meta in a RUNNING state."""
    return {
        "key": "0000000-000000000000000",
        "doi": "10.15468/dl.4fm52d",
        "license": "http://creativecommons.org/licenses/by-nc/4.0/legalcode",
        "request": {
            "predicate": {
                "type": "and",
                "predicates": [
                    {
                        "type": "equals",
                        "key": "TAXON_KEY",
                        "value": "7707728",
                        "matchCase": False,
                    },
                    {
                        "type": "not",
                        "predicate": {
                            "type": "in",
                            "key": "DEGREE_OF_ESTABLISHMENT",
                            "values": ["cultivated"],
                            "matchCase": False,
                        },
                    },
                ],
            },
            "sendNotification": True,
            "format": "SIMPLE_CSV",
            "type": "OCCURRENCE",
            "verbatimExtensions": [],
        },
        "created": "2024-01-17T14:18:26.685+00:00",
        "modified": "2024-01-17T14:31:12.445+00:00",
        "eraseAfter": "2024-07-17T14:18:26.641+00:00",
        "status": "RUNNING",
        "downloadLink": "https://api.gbif.org/v1/occurrence/download/request/"
        "0000000-000000000000000.zip",
        "size": 50765,
        "totalRecords": 1,
        "numberDatasets": 1,
    }


@pytest.fixture(name="gbif_download_meta_success")
def fixture_gbif_download_meta_success(gbif_download_meta_running) -> dict:
    """Sample GBIF download meta in a SUCCEEDED state."""
    return {**gbif_download_meta_running, "status": "SUCCEEDED"}


@pytest.fixture(name="gbif_download_meta_failed")
def fixture_gbif_download_meta_failed(gbif_download_meta_running) -> dict:
    """Sample GBIF download meta in a FAILED state."""
    return {**gbif_download_meta_running, "status": "FAILED"}


@pytest.fixture(name="mock_download_meta")
def fixture_mock_download_meta(monkeypatch, gbif_download_meta_success: dict):
    """Mock pygbif.occ.download_meta to return a successful download meta."""

    def _mock_download_meta(*args, **kwargs):
        return gbif_download_meta_success

    monkeypatch.setattr(occ, "download_meta", _mock_download_meta)


@pytest.fixture(name="mock_download_get")
def fixture_mock_download_get(monkeypatch):
    """Mock pygbif.occ.download_get to write a file to output_path."""

    def _mock_download_get(*args, **kwargs):
        with open(args[1], "wb") as f:
            f.write(b"test")

    monkeypatch.setattr(occ, "download_get", _mock_download_get)


def test_init_gbif_download(monkeypatch, gbif_query, gbif_download_info):
    """Test init_gbif_download"""

    def mock_download(*args, **kwargs):
        return gbif_download_info

    monkeypatch.setattr(occ, "download", mock_download)

    download_key = init_gbif_download(gbif_query)

    assert download_key == "0000000-000000000000000"


def test_check_download_status(
    monkeypatch,
    gbif_download_info,
    gbif_download_meta_running,
    gbif_download_meta_success,
    gbif_download_meta_failed,
):
    """Test check_download_status"""

    def mock_download_meta_running(*args, **kwargs):
        return gbif_download_meta_running

    def mock_download_meta_succeeded(*args, **kwargs):
        return gbif_download_meta_success

    def mock_download_meta_failed(*args, **kwargs):
        return gbif_download_meta_failed

    monkeypatch.setattr(occ, "download_meta", mock_download_meta_running)
    assert check_download_status(gbif_download_info[0]) == "RUNNING"

    monkeypatch.setattr(occ, "download_meta", mock_download_meta_succeeded)
    assert check_download_status(gbif_download_info[0]) == "SUCCEEDED"

    monkeypatch.setattr(occ, "download_meta", mock_download_meta_failed)
    assert check_download_status(gbif_download_info[0]) == "FAILED"


def mock_download_get(
    key: str, output_path: str, *args, **kwargs  # pylint: disable=unused-argument
) -> None:
    """Mock pygbif.occurences.download_get to write a throwaway file."""

    with open(Path(output_path) / f"{key}.zip", "wb") as f:
        f.write(b"test")


def test_download_request_to_disk(
    tmp_path, mocker, gbif_download_info, gbif_download_meta_success
):
    """Test download_request_to_disk"""
    mocker.patch(f"{TESTED_MODULE}.occ.download_get", side_effect=mock_download_get)
    mocker.patch(
        f"{TESTED_MODULE}.occ.download_meta", return_value=gbif_download_meta_success
    )
    download_request_to_disk(gbif_download_info[0], tmp_path)
    correct_file = tmp_path / f"{gbif_download_info[0]}.zip"

    assert correct_file.exists()
    assert correct_file.stat().st_size > 0
    assert correct_file.with_suffix(".json").exists()
    assert correct_file.with_suffix(".json").stat().st_size > 0

    ## Pytest doesn't seem to allow you to rename files in the tmp_path directory
    ## This causes the below test to fail.
    download_request_to_disk(gbif_download_info[0], tmp_path, "unique_name")
    correct_file = tmp_path / "unique_name.zip"

    assert correct_file.exists()
    assert correct_file.stat().st_size > 0
    assert correct_file.with_suffix(".json").exists()
    assert correct_file.with_suffix(".json").stat().st_size > 0


def test_check_download_job_and_download_file(tmp_path, mocker, gbif_download_info):
    """Test check_download_job_and_download_file"""
    output_path = tmp_path / "tmp.zip"
    mocker.patch(f"{TESTED_MODULE}.check_download_status", return_value="FAILED")
    with pytest.raises(GbifDownloadFailure) as excinfo:
        check_download_job_and_download_file(gbif_download_info[0], output_path)

    assert "failed." in str(excinfo.value)


def test_unzip_and_rename(tmp_path):
    """Test unzip_and_rename"""
    # Create a temporary zip file
    file_path = tmp_path / "test.zip"
    with zipfile.ZipFile(file_path, "w") as zipf:
        zipf.writestr("test_dir/test.txt", "This is a test file")

    # Call the function
    unzip_and_rename(file_path)

    # Check if the unzipped file exists
    unzipped_dir = tmp_path / "test.parquet"
    assert unzipped_dir.exists()

    # Check if the original zip file has been removed
    assert not file_path.exists()

    # Check if the file within the unzipped directory exists
    unzipped_file = unzipped_dir / "test.txt"
    assert unzipped_file.exists()
