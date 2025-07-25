from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from pandas.testing import assert_frame_equal
from rasterio.transform import from_bounds
from shapely.geometry import Polygon

from src.utils.cog_utils import (
    process_era5,
    process_floodscan,
    process_imerg,
    process_seas5,
)
from src.utils.raster_utils import (
    fast_zonal_stats,
    fast_zonal_stats_runner,
    rasterize_admin,
    upsample_raster,
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


@pytest.fixture
def sample_transform():
    return from_bounds(0, 0, 3, 3, 3, 3)


def test_fast_zonal_stats(
    sample_raster, sample_admin_raster, dropped_admin_raster
):
    stats = ["mean", "max", "min", "median", "sum", "std", "count"]
    result = fast_zonal_stats(sample_raster, sample_admin_raster, stats=stats)
    assert len(result) == 3, "Incorrect number of zones"
    assert result[0]["mean"] == pytest.approx(8.0), "Incorrect mean for zone 1"
    assert result[1]["max"] == 4, "Incorrect max for zone 2"
    assert result[2]["min"] == 3, "Incorrect min for zone 3"
    assert result[0]["median"] == pytest.approx(
        8.0
    ), "Incorrect median for zone 1"
    assert result[1]["sum"] == 7, "Incorrect sum for zone 2"
    assert result[2]["std"] == pytest.approx(
        1.247219
    ), "Incorrect std for zone 3"
    assert result[0]["count"] == 3, "Incorrect count for zone 1"

    result_dropped = fast_zonal_stats(
        sample_raster, dropped_admin_raster, stats=stats
    )
    assert len(result_dropped) == 6, "Incorrect number of zones"
    # Same as before for the '0' region
    assert result_dropped[0]["mean"] == pytest.approx(
        8.0
    ), "Incorrect mean for zone 1"
    assert result_dropped[5]["median"] == 4, "Incorrect median for zone 5"
    assert result_dropped[4]["count"] == 0, "Incorrect count for zone 4"
    assert np.isnan(result_dropped[3]["mean"]), "Incorrect mean for sone 3"


@pytest.fixture
def sample_xarray_dataarray_with_date():
    data = np.array(
        [
            [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]],
            [
                [17, 18, 19, 20],
                [21, 22, 23, 24],
                [25, 26, 27, 28],
                [29, 30, 31, 32],
            ],
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
            [
                (-1, -1),
                (-0.5, -0.8),
                (0, -0.5),
                (-0.2, 0),
                (-0.8, 0.2),
                (-1, 0.5),
            ]
        ),  # Left region
        Polygon([(-0.2, 0.2), (0.2, 0.5), (0, 1), (-0.5, 0.8)]),  # Top region
        Polygon(
            [(0.2, -1), (1, -0.8), (0.8, 0), (1, 0.5), (0.5, 0.8), (0, 0.2)]
        ),  # Right region
    ]
    return gpd.GeoDataFrame(
        {"geometry": geometries, "ADM1_PCODE": ["LEFT", "TOP", "RIGHT"]}
    )


def test_rasterize_admin(
    sample_gdf_with_pcode, sample_xarray_dataarray_with_date
):
    da = sample_xarray_dataarray_with_date
    gdf = sample_gdf_with_pcode
    src_transform = da.rio.transform()
    src_width = da.rio.width
    src_height = da.rio.height
    admin_raster = rasterize_admin(
        gdf, src_width, src_height, src_transform, all_touched=False
    )

    # The pcode at index 1 is dropped because it doesn't overlap with the
    # centroid with any raster cells
    expected = np.array(
        [
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, np.nan, 2.0, np.nan],
            [np.nan, 0.0, 2.0, np.nan],
            [np.nan, np.nan, np.nan, np.nan],
        ]
    )
    np.testing.assert_array_equal(
        admin_raster, expected, "Incorrect rasterization result"
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


@pytest.fixture
def sample_gdf_with_pcode_na_last():
    geometries = [
        Polygon(
            [
                (-1, -1),
                (-0.5, -0.8),
                (0, -0.5),
                (-0.2, 0),
                (-0.8, 0.2),
                (-1, 0.5),
            ]
        ),  # Left region
        Polygon(
            [(0.2, -1), (1, -0.8), (0.8, 0), (1, 0.5), (0.5, 0.8), (0, 0.2)]
        ),  # Right region
        Polygon([(-0.2, 0.2), (0.2, 0.5), (0, 1), (-0.5, 0.8)]),  # Top region
    ]
    return gpd.GeoDataFrame(
        {"geometry": geometries, "ADM1_PCODE": ["LEFT", "RIGHT", "TOP"]}
    )


# Test the edge case where a pcode with na values (ie. no raster coverage)
# is the last pcode in the dataframe. This catches a potential error in
# how the `fast_zonal_stats` function interacts with the `rasterize_admin` outputs
# to make sure that all pcodes (even if na) are present in the output dataframe
def test_fast_zonal_stats_runner_with_na_last(
    sample_xarray_dataarray_with_date, sample_gdf_with_pcode_na_last
):
    # Call the function
    result = fast_zonal_stats_runner(
        ds=sample_xarray_dataarray_with_date,
        gdf=sample_gdf_with_pcode_na_last,
        adm_level=1,
        iso3="TST",
        save_to_database=False,
    )

    # Expected output
    expected_data = {
        "mean": [10.0, 9.0, np.nan, 26.0, 25.0, np.nan],
        "max": [10.0, 11.0, np.nan, 26.0, 27.0, np.nan],
        "min": [10.0, 7.0, np.nan, 26.0, 23.0, np.nan],
        "median": [10.0, 9.0, np.nan, 26.0, 25.0, np.nan],
        "sum": [10.0, 18.0, 0.0, 26.0, 50.0, 0.0],
        "std": [0.0, 2.0, np.nan, 0.0, 2.0, np.nan],
        "count": [1, 2, 0, 1, 2, 0],
        "valid_date": pd.date_range("2021-01-01", periods=2).repeat(3),
        "pcode": ["LEFT", "RIGHT", "TOP", "LEFT", "RIGHT", "TOP"],
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


@pytest.fixture
def simple_dataset():
    """Create a simple 10x10 test dataset with 1.0 degree resolution."""
    data = np.random.rand(10, 10)
    ds = xr.Dataset(
        data_vars={"data": (("y", "x"), data)},
        coords={
            "x": np.arange(-5, 5, 1.0),  # 1.0 degree resolution
            "y": np.arange(-5, 5, 1.0),
        },
    )
    ds.rio.write_crs("EPSG:4326", inplace=True)
    return ds


@pytest.fixture
def dataset_with_time(simple_dataset):
    """Add a time dimension to the simple dataset."""
    data = np.random.rand(3, 10, 10)  # 3 time steps
    ds = xr.Dataset(
        data_vars={"data": (("date", "y", "x"), data)},
        coords={
            "x": simple_dataset.x,
            "y": simple_dataset.y,
            "date": pd.date_range("2020-01-01", "2020-01-03"),
        },
    )
    ds.rio.write_crs("EPSG:4326", inplace=True)
    return ds


@pytest.fixture
def dataset_with_leadtime(dataset_with_time):
    """Add a leadtime dimension to create a 4D dataset."""
    data = np.random.rand(3, 2, 10, 10)  # 3 times, 2 leadtimes
    ds = xr.Dataset(
        data_vars={"data": (("date", "leadtime", "y", "x"), data)},
        coords={
            "x": dataset_with_time.x,
            "y": dataset_with_time.y,
            "date": dataset_with_time.date,
            "leadtime": ["1month", "2month"],
        },
    )
    ds.rio.write_crs("EPSG:4326", inplace=True)
    return ds


def dataset_with_date(date, attrs=None, leadtime=None):
    if leadtime:
        data = np.random.rand(1, len(leadtime), 4, 4)
        da = xr.DataArray(
            data,
            dims=["date", "leadtime", "y", "x"],
            coords={
                "date": date,
                "leadtime": ["1month"],
                "x": np.arange(-2, 2, 1.0),
                "y": np.arange(-2, 2, 1.0),
            },
        )
    else:
        data = np.random.rand(1, 4, 4)  # 3 times, 2 leadtimes
        da = xr.DataArray(
            data,
            dims=["date", "y", "x"],
            coords={
                "date": pd.to_datetime(date, format="%Y-%m-%d"),
                "x": np.arange(-2, 2, 1.0),
                "y": np.arange(-2, 2, 1.0),
            },
        )
    da.attrs = attrs
    return da


def test_basic_upsampling(dataset_with_time):
    """Test basic upsampling of a 2D dataset."""
    target_res = 0.5  # Upsample from 1.0 to 0.5 degrees
    result = upsample_raster(
        dataset_with_time, resampled_resolution=target_res
    )

    # Check output resolution
    assert result.rio.resolution()[0] == target_res

    # Check output dimensions doubled (since resolution halved)
    assert result.rio.width == dataset_with_time.rio.width * 2
    assert result.rio.height == dataset_with_time.rio.height * 2


def test_upsampling_with_time(dataset_with_time):
    """Test upsampling of a 3D dataset with time dimension."""
    target_res = 0.5
    result = upsample_raster(
        dataset_with_time, resampled_resolution=target_res
    )

    # Check time dimension preserved
    assert "date" in result.dims
    assert len(result.date) == len(dataset_with_time.date)

    # Check spatial dimensions
    assert result.rio.resolution()[0] == target_res
    assert result.rio.width == dataset_with_time.rio.width * 2
    assert result.rio.height == dataset_with_time.rio.height * 2


def test_upsampling_with_leadtime(dataset_with_leadtime):
    """Test upsampling of a 4D dataset with time and leadtime dimensions."""
    target_res = 0.5
    result = upsample_raster(
        dataset_with_leadtime, resampled_resolution=target_res
    )

    # Check temporal dimensions preserved
    assert "date" in result.dims
    assert "leadtime" in result.dims
    assert len(result.date) == len(dataset_with_leadtime.date)
    assert len(result.leadtime) == len(dataset_with_leadtime.leadtime)

    # Check spatial dimensions
    assert result.rio.resolution()[0] == target_res
    assert result.rio.width == dataset_with_leadtime.rio.width * 2
    assert result.rio.height == dataset_with_leadtime.rio.height * 2


@pytest.mark.parametrize(
    "attrs, date",
    [
        ({"year_valid": 2020, "month_valid": 1, "leadtime": 0}, "2020-01-01"),
        ({"year_valid": 2020, "month_valid": 1, "leadtime": 1}, "2020-01-02"),
    ],
)
def test_process_seas5_valid(attrs, date, simple_gdf_with_one_pcode):
    da = dataset_with_date(date=date, attrs=attrs, leadtime=["1_month"])
    da.attrs = attrs

    with patch("src.utils.cog_utils.get_cog_da", return_value=da):
        da_in = process_seas5("cog_url", "local")
        assert pd.to_datetime(da_in.date)[0].year == da.attrs["year_valid"]
        assert pd.to_datetime(da_in.date)[0].month == da.attrs["month_valid"]
        assert int(da_in.leadtime) == da.attrs["leadtime"]

        result = fast_zonal_stats_runner(
            ds=da_in,
            gdf=simple_gdf_with_one_pcode,
            adm_level=1,
            iso3="TST",
            save_to_database=False,  # Ensure we're not trying to save to a database
        )
        assert result["valid_date"][0] == (
            f"{da.attrs['year_valid']}-"
            f"{str(da.attrs['month_valid']).zfill(2)}-01"
        )


@pytest.mark.parametrize(
    "attrs, date, error_message",
    [
        (
            {"year_valid": 2020, "month_valid": 1},
            "2020-01-01",
            "KeyError: 'leadtime'",
        ),
        (
            {"year_valid": 2020, "leadtime": 1},
            "2020-01-01",
            "KeyError: 'month_valid'",
        ),
        (
            {"month_valid": 1, "leadtime": 1},
            "2020-01-01",
            "KeyError: 'year_valid'",
        ),
    ],
)
def test_process_seas5_raises_error(attrs, date, error_message):
    da = dataset_with_date(date=date, attrs=attrs, leadtime=["1_month"])
    da.attrs = attrs

    """Test cases that should fail validation with specific errors"""
    with pytest.raises(KeyError) as excinfo:
        with patch("src.utils.cog_utils.get_cog_da", return_value=da):
            process_seas5("cog_url", "local")
            assert error_message in str(excinfo.value)


@pytest.mark.parametrize(
    "attrs, date",
    [
        ({"year_valid": 2020, "month_valid": 1}, "2020-01-01"),
        ({"year_valid": 2020, "month_valid": 1}, "2020-01-02"),
    ],
)
def test_process_era5_valid(attrs, date, simple_gdf_with_one_pcode):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    with patch("src.utils.cog_utils.get_cog_da", return_value=da):
        da_in = process_era5("cog_url", "local")
        assert pd.to_datetime(da_in.date)[0].year == da.attrs["year_valid"]
        assert pd.to_datetime(da_in.date)[0].month == da.attrs["month_valid"]

        result = fast_zonal_stats_runner(
            ds=da_in,
            gdf=simple_gdf_with_one_pcode,
            adm_level=1,
            iso3="TST",
            save_to_database=False,  # Ensure we're not trying to save to a database
        )
        assert result["valid_date"][0] == (
            f"{da.attrs['year_valid']}-"
            f"{str(da.attrs['month_valid']).zfill(2)}-01"
        )


@pytest.mark.parametrize(
    "attrs, date, error_message",
    [
        (
            {"year_valid": 2020, "leadtime": 1},
            "2020-01-01",
            "KeyError: 'month_valid'",
        ),
        (
            {"month_valid": 1, "leadtime": 1},
            "2020-01-01",
            "KeyError: 'year_valid'",
        ),
    ],
)
def test_process_era5_raises_error(attrs, date, error_message):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    """Test cases that should fail validation with specific errors"""
    with pytest.raises(KeyError) as excinfo:
        with patch("src.utils.cog_utils.get_cog_da", return_value=da):
            process_era5("cog_url", "local")
            assert error_message in str(excinfo.value)


@pytest.mark.parametrize(
    "attrs, date",
    [
        (
            {"year_valid": 2020, "month_valid": 2, "date_valid": 1},
            "2020-01-01",
        ),
        (
            {"year_valid": 2020, "month_valid": 12, "date_valid": 1},
            "2020-01-01",
        ),
    ],
)
def test_process_imerg_valid(attrs, date, simple_gdf_with_one_pcode):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    with patch("src.utils.cog_utils.get_cog_da", return_value=da):
        da_in = process_imerg("cog_url", "local")
        assert pd.to_datetime(da_in.date)[0].year == da.attrs["year_valid"]
        assert pd.to_datetime(da_in.date)[0].month == da.attrs["month_valid"]
        assert pd.to_datetime(da_in.date)[0].day == da.attrs["date_valid"]

        result = fast_zonal_stats_runner(
            ds=da_in,
            gdf=simple_gdf_with_one_pcode,
            adm_level=1,
            iso3="TST",
            save_to_database=False,  # Ensure we're not trying to save to a database
        )
        assert result["valid_date"][0] == (
            f"{da.attrs['year_valid']}-"
            f"{str(da.attrs['month_valid']).zfill(2)}-01"
        )


@pytest.mark.parametrize(
    "attrs, date, error_message",
    [
        ({"year_valid": 2020}, "2020-01-01", "KeyError: 'month_valid'"),
        ({"month_valid": 1}, "2020-01-01", "KeyError: 'year_valid'"),
    ],
)
def test_process_imerg_raises_error(attrs, date, error_message):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    """Test cases that should fail validation with specific errors"""
    with pytest.raises(KeyError) as excinfo:
        with patch("src.utils.cog_utils.get_cog_da", return_value=da):
            process_imerg("cog_url", "local")
            assert error_message in str(excinfo.value)


@pytest.fixture
def simple_gdf_with_one_pcode():
    geometry = Polygon([(-1, -1), (-1, 1), (1, 1), (1, -1), (-1, -1)])
    return gpd.GeoDataFrame({"geometry": [geometry], "ADM1_PCODE": "LEFT"})


@pytest.mark.parametrize(
    "attrs, date",
    [
        (
            {"year_valid": 2020, "month_valid": 1, "date_valid": 1},
            "2020-01-01",
        ),
        (
            {"year_valid": 2020, "month_valid": 12, "date_valid": 30},
            "2020-01-02",
        ),
    ],
)
def test_process_floodscan_valid(attrs, date, simple_gdf_with_one_pcode):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    with patch("src.utils.cog_utils.get_cog_da", return_value=da):
        da_in = process_floodscan("cog_url", "local")
        assert pd.to_datetime(da_in.date)[0].year == da.attrs["year_valid"]
        assert pd.to_datetime(da_in.date)[0].month == da.attrs["month_valid"]
        assert pd.to_datetime(da_in.date)[0].day == da.attrs["date_valid"]

        result = fast_zonal_stats_runner(
            ds=da_in,
            gdf=simple_gdf_with_one_pcode,
            adm_level=1,
            iso3="TST",
            save_to_database=False,  # Ensure we're not trying to save to a database
        )
        assert result["valid_date"][0] == (
            f"{da.attrs['year_valid']}-"
            f"{str(da.attrs['month_valid']).zfill(2)}-"
            f"{str(da.attrs['date_valid']).zfill(2)}"
        )


@pytest.mark.parametrize(
    "attrs, date, error_message",
    [
        (
            {"year_valid": 2020, "date_valid": 1},
            "2020-01-01",
            "KeyError: 'month_valid'",
        ),
        (
            {"month_valid": 1, "date_valid": 2},
            "2020-01-01",
            "KeyError: 'year_valid'",
        ),
        (
            {"year_valid": 2020, "month_valid": 3},
            "2020-01-01",
            "KeyError: 'date_valid'",
        ),
    ],
)
def test_process_floodscan_raises_error(attrs, date, error_message):
    da = dataset_with_date(date=date, attrs=attrs)
    da.attrs = attrs

    """Test cases that should fail validation with specific errors"""
    with pytest.raises(KeyError) as excinfo:
        with patch("src.utils.cog_utils.get_cog_da", return_value=da):
            process_floodscan("cog_url", "local")
            assert error_message in str(excinfo.value)
