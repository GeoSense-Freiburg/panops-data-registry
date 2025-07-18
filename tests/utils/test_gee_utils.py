"""Tests for Google Earth Engine utility functions."""

import os
from pathlib import Path

import ee
import pytest

# Set PROJECT_ROOT environment variable before importing modules that need it
os.environ.setdefault("PROJECT_ROOT", str(Path(__file__).parent.parent.parent))

from src.utils.gee_utils import ee_init, get_crs_and_transform


@pytest.fixture(name="test_image", scope="session")
def fixture_test_image() -> ee.Image:
    """
    Fixture that provides an image for GEE Image-related tests.

    Returns:
        ee.Image: Image from Google Earth Engine
    """
    # Initialize Earth Engine for testing
    ee_init("global-traits", high_volume=True)

    # Get the ALOS CHILI image - this is a reliable, well-known dataset in GEE
    test_image = ee.Image("CSP/ERGo/1_0/Global/ALOS_CHILI").select("constant")

    return test_image


def test_get_crs_and_transform(test_image):
    """Test that get_crs_and_transform returns CRS and transform as expected."""
    crs, transform = get_crs_and_transform(test_image)

    # Test that we get the expected types
    assert isinstance(crs, str), f"CRS should be string, got {type(crs)}"
    assert isinstance(transform, str), (
        f"Transform should be string, got {type(transform)}"
    )

    # Test that values are not empty
    assert len(crs) > 0, "CRS should not be empty"
    assert len(transform) > 0, "Transform should not be empty"

    # Test that CRS is in expected format (EPSG for ALOS CHILI)
    assert crs == "EPSG:4326", f"Expected EPSG:4326 for ALOS CHILI, got {crs}"

    # Test that transform contains expected affine transformation structure
    assert "PARAM_MT" in transform, "Transform should contain PARAM_MT"
    assert "Affine" in transform, "Transform should contain Affine"
