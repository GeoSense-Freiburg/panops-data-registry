"""Tests for the GCS utility functions."""

from pathlib import Path
from unittest.mock import Mock, patch

from google.cloud import storage
from pytest import LogCaptureFixture

from src.utils.gcs_utils import (
    download_blob,
    download_blob_if_exists,
    download_blobs,
    download_bucket,
)


def test_download_blob(tmp_path: Path):
    """
    Test function for downloading a blob from Google Cloud Storage.

    Args:
        tmp_path (Path): The temporary directory path where the downloaded file will be saved.
    """
    # Mock the blob
    blob = Mock(spec=storage.Blob)
    blob.name = "test_blob.txt"
    out_file_path = tmp_path / Path(blob.name).name

    # Mock the download_to_filename method
    blob.download_to_filename = Mock()

    # Call the function with the mock blob and a temporary directory
    download_blob(blob, tmp_path)

    # Assert that the download_to_filename method was called with the correct argument
    blob.download_to_filename.assert_called_once_with(out_file_path)


def test_download_blob_existing_file(tmp_path: Path, caplog: LogCaptureFixture):
    """
    Test case for the download_blob function when the file already exists.

    Args:
        tmp_path (Path): The temporary directory path for the test.
        caplog (LogCaptureFixture): The fixture for capturing log messages.
    """

    # Mock the blob
    blob = Mock(spec=storage.Blob)
    blob.name = "test_blob.txt"
    out_file_path = tmp_path / Path(blob.name).name

    # Create a dummy file at the output path
    with open(out_file_path, "w", encoding="utf-8") as f:
        f.write("dummy file")

    # Mock the download_to_filename method
    blob.download_to_filename = Mock()

    # Assert that the log warning is called when the file already exists
    with caplog.at_level("WARNING"):
        download_blob(blob, tmp_path)
        assert "File test_blob.txt already exists at" in caplog.text


def test_download_blobs(tmp_path: Path):
    """
    Test case for the download_blobs function.

    Args:
        tmp_path (Path): Temporary directory path for downloading blobs.

    Raises:
        AssertionError: If the download_blob function is not called the correct number of times
                        or with the correct arguments.
    """
    # Mock the blobs
    blobs: list[storage.Blob] = [Mock(spec=storage.Blob) for _ in range(3)]
    for i, blob in enumerate(blobs):
        blob.name = f"test_blob_{i}.txt"

    # Mock the download_blob function
    with patch("src.utils.gcs_utils.download_blob") as mock_download_blob:
        # Call the function with the mock blobs and a temporary directory
        download_blobs(blobs, tmp_path)

        # Assert that the download_blob function was called the correct number of times
        assert mock_download_blob.call_count == len(blobs)

        # Assert that the download_blob function was called with the correct arguments
        for i, blob in enumerate(blobs):
            mock_download_blob.assert_any_call(blob, tmp_path)


def test_download_blob_if_exists_blob_exists_overwrite_true(
    tmp_path: Path, caplog: LogCaptureFixture
):
    """
    Test case for the download_blob_if_exists function when the blob exists and overwrite is set to True.

    Args:
        tmp_path (Path): The temporary directory path for downloading the blob.
        caplog (LogCaptureFixture): The fixture for capturing log messages.
    """
    # Mock the blob and bucket
    blob = Mock(spec=storage.Blob)
    blob.exists.return_value = True
    bucket = Mock(spec=storage.Bucket)
    bucket.blob.return_value = blob

    # Mock the download_blob function
    with patch("src.utils.gcs_utils.download_blob") as mock_download_blob:
        # Call the function with overwrite=True
        download_blob_if_exists("test", bucket, tmp_path, overwrite=True)

        # Assert that the download_blob function was called
        mock_download_blob.assert_called_once_with(blob, tmp_path)


def test_download_blob_if_exists_blob_exists_overwrite_false_file_exists(
    tmp_path: Path, caplog: LogCaptureFixture
):
    """
    Test case for the `download_blob_if_exists` function when the blob exists,
    overwrite is set to False, and the file already exists.

    Args:
        tmp_path (Path): Temporary path for creating the dummy file.
        caplog (LogCaptureFixture): Fixture for capturing log messages.
    """
    # Mock the blob and bucket
    blob = Mock(spec=storage.Blob)
    blob.exists.return_value = True
    bucket = Mock(spec=storage.Bucket)
    bucket.blob.return_value = blob

    # Create a dummy file at the output path
    with open(tmp_path / "test.tif", "w", encoding="utf-8") as f:
        f.write("dummy file")

    # Mock the download_blob function
    with patch("src.utils.gcs_utils.download_blob") as mock_download_blob:
        # Call the function with overwrite=False
        download_blob_if_exists("test", bucket, tmp_path, overwrite=False)

        # Assert that the download_blob function was not called
        mock_download_blob.assert_not_called()

        # Assert that the log info is called when the file already exists
        assert "File test.tif already exists at" in caplog.text


def test_download_blob_if_exists_blob_not_exists_blobs_with_same_stem_exist(
    tmp_path: Path, caplog: LogCaptureFixture
):
    """
    Test case for the `download_blob_if_exists` function when the blob does not exist
    but blobs with the same stem exist in the bucket.

    Args:
        tmp_path (str): Temporary path for downloading the blob.
        caplog (pytest.LogCaptureFixture): Fixture for capturing log output.
    """
    # Mock the blob, blobs and bucket
    blob = Mock(spec=storage.Blob)
    blob.exists.return_value = False
    blobs = [Mock(spec=storage.Blob) for _ in range(3)]
    bucket = Mock(spec=storage.Bucket)
    bucket.blob.return_value = blob
    bucket.list_blobs.return_value = blobs

    # Mock the download_blobs function
    with patch("src.utils.gcs_utils.download_blobs") as mock_download_blobs:
        # Call the function
        download_blob_if_exists("test", bucket, tmp_path)

        # Assert that the download_blobs function was called
        mock_download_blobs.assert_called_once_with(blobs, tmp_path)

        # Assert that the log warning is called when the file is not found in the bucket
        assert (
            "File test.tif not found in bucket. Checking if split into parts..."
            in caplog.text
        )


def test_download_blob_if_exists_blob_not_exists_no_blobs_with_same_stem_exist(
    tmp_path: Path, caplog: LogCaptureFixture
):
    """
    Test case for the `download_blob_if_exists` function when the blob does not exist
    and no blobs with the same stem exist in the bucket.

    Args:
        tmp_path (str): Temporary path for downloading the blob.
        caplog (pytest.LogCaptureFixture): Fixture for capturing log messages.
    """
    # Mock the blob and bucket
    blob = Mock(spec=storage.Blob)
    blob.exists.return_value = False
    bucket = Mock(spec=storage.Bucket)
    bucket.blob.return_value = blob
    bucket.list_blobs.return_value = []

    # Call the function
    download_blob_if_exists("test", bucket, tmp_path)

    # Assert that the log error is called when the file is not found in the bucket
    assert "File test.tif not found in bucket." in caplog.text


def test_download_bucket(tmp_path: Path):
    """
    Test case for the download_bucket function.

    Args:
        tmp_path (Path): Temporary directory path for downloading blobs.

    Raises:
        AssertionError: If the download_blob function is not called the correct number of times
                        or with the correct arguments.
    """
    # Mock the storage client, bucket, and blobs
    storage_client = Mock(spec=storage.Client)
    bucket = Mock(spec=storage.Bucket)
    blobs: list[storage.Blob] = [Mock(spec=storage.Blob) for _ in range(3)]
    for i, blob in enumerate(blobs):
        blob.name = f"test_blob_{i}.txt"

    # Mock the get_bucket and list_blobs methods
    storage_client.get_bucket.return_value = bucket
    bucket.list_blobs.return_value = blobs

    # Mock the download_blob function and the storage client
    with patch("src.utils.gcs_utils.download_blob") as mock_download_blob, patch(
        "src.utils.gcs_utils.storage.Client", return_value=storage_client
    ):
        # Call the function with the mock storage client, bucket name, and a temporary directory
        download_bucket("test_bucket", tmp_path)

        # Assert that the download_blob function was called the correct number of times
        assert mock_download_blob.call_count == len(blobs)

        # Assert that the download_blob function was called with the correct arguments
        for blob in blobs:
            mock_download_blob.assert_any_call(blob, tmp_path)
