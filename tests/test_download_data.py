"""Tests for the download_data module."""
from pathlib import Path

import requests

from src.data.setup_raw.download_data import (
    download_file,
    download_files,
    get_datasets_download_info,
    make_dir_if_new,
)


def test_make_dir_if_new(tmp_path):
    """Test the make_dir_if_new function."""
    # Arrange ===
    temp_dir = Path(tmp_path)
    new_dir = temp_dir / "new_dir"

    # Act ===
    make_dir_if_new(new_dir)

    # Assert ===
    assert new_dir.exists()
    assert new_dir.is_dir(), f"{new_dir} should be a directory but is not."


def test_download_file(tmp_path):
    """Test the download_file function."""
    # Arrange ===
    temp_dir = Path(tmp_path)
    url = "https://getsamplefiles.com/download/txt/sample-1.txt"
    expected_filename = "sample-1.txt"

    response = requests.get(url, timeout=10)
    expected_file_contents = response.content

    # Act ===
    file_path = download_file(url, temp_dir)
    downloaded_file = temp_dir / expected_filename
    with open(downloaded_file, "rb") as file:
        downloaded_file_contents = file.read()

    # Assert ===
    # Verify the downloaded file exists
    assert downloaded_file.exists(), f"{downloaded_file} should exist, but it doesn't."
    assert (
        downloaded_file.is_file()
    ), f"{downloaded_file} should be a file, but it isn't."

    # Confirm file path is correctly returned.
    assert file_path == downloaded_file

    # Verify that the contents of the local and source files match
    assert downloaded_file_contents == expected_file_contents


def test_download_files_happy_path(tmp_path):
    """Test the download_files function."""
    # Arrange ===
    temp_dir = Path(tmp_path)
    urls = [
        "https://getsamplefiles.com/download/txt/sample-1.txt",
        "https://getsamplefiles.com/download/txt/sample-2.txt",
        "https://getsamplefiles.com/download/txt/sample-3.txt",
    ]
    expected_filenames = ["sample-1.txt", "sample-2.txt", "sample-3.txt"]

    responses = []
    expected_file_contents = []
    for url in urls:
        response = requests.get(url, timeout=10)
        responses.append(response)
        expected_file_contents.append(response.content)

    # Act ===
    download_files(urls, [temp_dir] * len(urls))

    # Assert ===
    for i, expected_filename in enumerate(expected_filenames):
        downloaded_file = temp_dir / expected_filename
        with open(downloaded_file, "rb") as file:
            downloaded_file_contents = file.read()

        # Verify the downloaded file exists
        assert (
            downloaded_file.exists()
        ), f"{downloaded_file} should exist, but it doesn't."
        assert (
            downloaded_file.is_file()
        ), f"{downloaded_file} should be a file, but it isn't."

        # Verify that the contents of the local and source files match
        assert downloaded_file_contents == expected_file_contents[i]


def test_get_datasets_download_info():
    """Test the _get_datasets_download_info function."""
    # Arrange ===
    datasets = [
        {"url": "https://example.com/dataset1", "odir": Path("output1")},
        {"url": "https://example.com/dataset2", "odir": Path("output2")},
        {"url": "https://example.com/dataset3", "odir": Path("output3")},
    ]
    expected_urls = [
        "https://example.com/dataset1",
        "https://example.com/dataset2",
        "https://example.com/dataset3",
    ]
    expected_odirs = [
        Path("output1"),
        Path("output2"),
        Path("output3"),
    ]

    # Act ===
    urls, odirs = get_datasets_download_info(datasets)

    # Assert ===
    assert urls == expected_urls
    assert odirs == expected_odirs
