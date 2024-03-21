"""Test modis.process_data"""

# pyright: reportOptionalSubscript=false
import dask.distributed
import ee
import pytest
import xarray as xr

from src.modis.process_data import get_modis_ic, process_modis_ic, setup


def test_setup():
    """Test setup"""
    client = setup()
    assert isinstance(client, dask.distributed.Client)
    assert client.status == "running"
    client.close()
    assert client.status == "closed"


@pytest.fixture(name="modis_cfg")
def fixture_modis_cfg():
    """Fixture for MODIS configuration"""
    return {
        "product": "MODIS/006/MOD09GA",
        "bands": ["sur_refl_b01", "sur_refl_b02"],
        "date_start": "2001-01-01",
        "date_end": "2001-01-2",
        "qa_band": "state_1km",
        "crs": "EPSG:4326",
        "scale": 1,
    }


def test_get_modis_ic(modis_cfg):
    """Test get_modis_ic"""
    modis = get_modis_ic(modis_cfg)
    assert isinstance(modis, ee.ImageCollection)
    assert modis.size().getInfo() > 0
    assert modis.first().getInfo() is not None
    assert modis.first().getInfo()["bands"] is not None
    assert modis.first().getInfo()["bands"][0]["id"] == "sur_refl_b01"
    assert modis.first().getInfo()["bands"][1]["id"] == "sur_refl_b02"
    assert modis.first().getInfo()["properties"]["system:time_start"] is not None
    assert modis.first().getInfo()["properties"]["system:time_end"] is not None


@pytest.fixture(name="modis_ic")
def fixture_modis_ic(modis_cfg):
    """Fixture for MODIS ImageCollection"""
    return get_modis_ic(modis_cfg)


def test_process_modis_ic(modis_ic, modis_cfg):
    """Test process_modis_ic"""
    ds = process_modis_ic(modis_ic, modis_cfg)
    assert isinstance(ds, xr.Dataset)
    assert ds.time.size > 0
    assert ds.sur_refl_b01.size > 0
    assert ds.sur_refl_b02.size > 0
    # assert that the time dimension is monthly and contains only 12 months
    assert ds.time.to_index().month.unique().size == 12
