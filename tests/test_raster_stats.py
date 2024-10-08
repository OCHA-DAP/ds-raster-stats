import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from pandas.testing import assert_frame_equal
from shapely.geometry import Polygon

from src.utils.raster_utils import (
    fast_zonal_stats,
    fast_zonal_stats_runner,
    rasterize_admin,
)


@pytest.fixture
def sample_raster():
    return np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])


@pytest.fixture
def sample_xarray_dataset(sample_raster):
    ds = xr.Dataset(
        {"data": (["y", "x"], sample_raster)},
        coords={"x": np.linspace(-1, 1, 3), "y": np.linspace(1, -1, 3)},
    )
    ds.rio.write_crs("EPSG:4326", inplace=True)
    return ds


@pytest.fixture
def sample_admin_raster():
    return np.array([[1, 1, 2], [1, 2, 2], [0, 0, 0]])


# Capture cases where some admins aren't in the output raster
# since they don't have any raster centroids. This just looks
# like non-consecutive labels for the regions
@pytest.fixture
def dropped_admin_raster():
    return np.array([[1, 1, 2], [5, 2, 2], [0, 0, 0]])


@pytest.fixture
def sample_gdf():
    geometry = Polygon([(-1, -1), (-1, 1), (1, 1), (1, -1), (-1, -1)])
    return gpd.GeoDataFrame({"geometry": [geometry]})


def test_fast_zonal_stats(sample_raster, sample_admin_raster, dropped_admin_raster):
    stats = ["mean", "max", "min", "median", "sum", "std", "count"]
    result = fast_zonal_stats(sample_raster, sample_admin_raster, stats)
    assert len(result) == 3, "Incorrect number of zones"
    assert result[0]["mean"] == pytest.approx(8.0), "Incorrect mean for zone 1"
    assert result[1]["max"] == 4, "Incorrect max for zone 2"
    assert result[2]["min"] == 3, "Incorrect min for zone 3"
    assert result[0]["median"] == pytest.approx(8.0), "Incorrect median for zone 1"
    assert result[1]["sum"] == 7, "Incorrect sum for zone 2"
    assert result[2]["std"] == pytest.approx(1.247219), "Incorrect std for zone 3"
    assert result[0]["count"] == 3, "Incorrect count for zone 1"

    result_dropped = fast_zonal_stats(sample_raster, dropped_admin_raster, stats)
    assert len(result_dropped) == 6, "Incorrect number of zones"
    # Same as before for the '0' region
    assert result_dropped[0]["mean"] == pytest.approx(8.0), "Incorrect mean for zone 1"
    assert result_dropped[5]["median"] == 4, "Incorrect median for zone 5"
    assert result_dropped[4]["count"] == 0, "Incorrect count for zone 4"
    assert np.isnan(result_dropped[3]["mean"]), "Incorrect mean for sone 3"


def test_rasterize_admin(sample_gdf, sample_transform):
    src_width, src_height = 3, 3
    result = rasterize_admin(sample_gdf, src_width, src_height, sample_transform)

    expected = np.array(
        [
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
            [0, np.nan, np.nan],
        ]
    )
    np.testing.assert_array_equal(result, expected, "Incorrect rasterization result")


@pytest.fixture
def sample_xarray_dataarray_with_date():
    data = np.array(
        [
            [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]],
            [[17, 18, 19, 20], [21, 22, 23, 24], [25, 26, 27, 28], [29, 30, 31, 32]],
        ]
    )
    da = xr.DataArray(
        data,
        dims=["date", "y", "x"],
        coords={
            "date": pd.date_range("2021-01-01", periods=2),
            "x": np.linspace(-1, 1, 4),
            "y": np.linspace(1, -1, 4),
        },
    )
    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


@pytest.fixture
def sample_gdf_with_pcode():
    geometries = [
        Polygon(
            [(-1, -1), (-0.5, -0.8), (0, -0.5), (-0.2, 0), (-0.8, 0.2), (-1, 0.5)]
        ),  # Left region
        Polygon([(-0.2, 0.2), (0.2, 0.5), (0, 1), (-0.5, 0.8)]),  # Top region
        Polygon(
            [(0.2, -1), (1, -0.8), (0.8, 0), (1, 0.5), (0.5, 0.8), (0, 0.2)]
        ),  # Right region
    ]
    return gpd.GeoDataFrame(
        {"geometry": geometries, "ADM1_PCODE": ["LEFT", "TOP", "RIGHT"]}
    )


def test_fast_zonal_stats_runner(
    sample_xarray_dataarray_with_date, sample_gdf_with_pcode
):
    # Call the function
    result = fast_zonal_stats_runner(
        ds=sample_xarray_dataarray_with_date,
        gdf=sample_gdf_with_pcode,
        adm_level=1,
        iso3="TST",
        save_to_database=False,  # Ensure we're not trying to save to a database
    )

    # Expected output
    expected_data = {
        "mean": [10.0, np.nan, 9.0, 26.0, np.nan, 25.0],
        "max": [10.0, np.nan, 11.0, 26.0, np.nan, 27.0],
        "min": [10.0, np.nan, 7.0, 26.0, np.nan, 23.0],
        "median": [10.0, np.nan, 9.0, 26.0, np.nan, 25.0],
        "sum": [10.0, 0.0, 18.0, 26.0, 0.0, 50.0],
        "std": [0.0, np.nan, 2.0, 0.0, np.nan, 2.0],
        "count": [1, 0, 2, 1, 0, 2],
        "valid_date": pd.date_range("2021-01-01", periods=2).repeat(3),
        "pcode": ["LEFT", "TOP", "RIGHT", "LEFT", "TOP", "RIGHT"],
        "adm_level": [1, 1, 1, 1, 1, 1],
        "iso3": ["TST", "TST", "TST", "TST", "TST", "TST"],
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert equality
    assert_frame_equal(result, expected_df, check_dtype=False)

    # Additional checks
    assert len(result) == 6, "Incorrect number of rows"
    assert len(result.pcode.unique()) == 3, "Not all pcodes included"
    assert set(result.columns) == set(
        expected_df.columns
    ), "Mismatch in DataFrame columns"
    assert result["iso3"].unique() == ["TST"], "Incorrect ISO3 code"
    assert result["adm_level"].unique() == [1], "Incorrect admin level"
