import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from shapely.geometry import Polygon, box

import src.utils.zonal_utils as zonal_utils
from src.utils.raster_utils import fast_zonal_stats_runner, upsample_raster
from src.utils.zonal_utils import (
    _weighted_median,
    build_weights,
    clip_raster,
    validate_stats_df,
    weighted_zonal_stats,
    zonal_stats_runner,
)


@pytest.fixture(autouse=True)
def tmp_weights_cache(tmp_path, monkeypatch):
    """Keep the weight-matrix disk cache inside the test tmp dir."""
    monkeypatch.setattr(
        zonal_utils, "WEIGHTS_CACHE_DIR", str(tmp_path / "weights")
    )


def make_da(data, res=1.0, x0=-5, y0=5, dims=("date", "y", "x"), coords=None):
    """Build a DataArray on a regular grid with pixel centers offset
    half a pixel from the (x0, y0) top-left corner."""
    ny, nx = data.shape[-2], data.shape[-1]
    base_coords = {
        "x": x0 + res * (np.arange(nx) + 0.5),
        "y": y0 - res * (np.arange(ny) + 0.5),
    }
    if coords:
        base_coords.update(coords)
    da = xr.DataArray(data, dims=dims, coords=base_coords)
    da.rio.write_crs("EPSG:4326", inplace=True)
    return da


@pytest.fixture
def random_gdf():
    """Polygons NOT aligned to any pixel grid."""
    geometries = [
        Polygon([(-4.3, -3.2), (-0.7, -4.1), (0.4, -0.9), (-3.8, 0.6)]),
        Polygon([(-0.2, 0.3), (3.6, 1.1), (2.9, 4.2), (-1.4, 3.5)]),
        Polygon([(1.1, -4.4), (4.4, -3.7), (3.8, -0.4)]),
    ]
    return gpd.GeoDataFrame(
        {"geometry": geometries, "ADM1_PCODE": ["A1", "A2", "A3"]},
        crs="EPSG:4326",
    )


def test_weighted_stats_match_exactextract_builtins(random_gdf):
    """Our sparse-matrix stats must agree with exactextract's own
    coverage-weighted operations computed directly per slice."""
    rng = np.random.default_rng(7)
    data = rng.gamma(2.0, 50.0, size=(4, 10, 10))
    da = make_da(data)

    W = build_weights(random_gdf, "ADM1_PCODE", da)
    stats = weighted_zonal_stats(data, W, scale=1.0)

    for k in range(data.shape[0]):
        rast = NumPyRasterSource(
            data[k], xmin=-5, ymin=-5, xmax=5, ymax=5, srs_wkt=None
        )
        expected = exact_extract(
            rast,
            random_gdf,
            ["mean", "sum", "count", "stdev", "median", "min", "max"],
            output="pandas",
        )
        np.testing.assert_allclose(
            stats["mean"][:, k], expected["mean"], rtol=1e-5
        )
        np.testing.assert_allclose(
            stats["sum"][:, k], expected["sum"], rtol=1e-5
        )
        np.testing.assert_allclose(
            stats["std"][:, k], expected["stdev"], rtol=1e-5
        )
        np.testing.assert_allclose(
            stats["min"][:, k], expected["min"], rtol=1e-6
        )
        np.testing.assert_allclose(
            stats["max"][:, k], expected["max"], rtol=1e-6
        )
        # exactextract's median op resolves the cumulative-weight
        # boundary differently (takes the lower value); ours follows
        # the np.median convention, so compare against a brute-force
        # weighted median (each pixel replicated by its coverage)
        expected_cov = exact_extract(
            rast, random_gdf, ["cell_id", "coverage"], output="pandas"
        )
        for a in range(len(random_gdf)):
            cell_ids = expected_cov["cell_id"][a]
            coverage = expected_cov["coverage"][a]
            replicated = np.repeat(
                data[k].ravel()[cell_ids],
                np.round(coverage * 100000).astype(int),
            )
            np.testing.assert_allclose(
                stats["median"][a, k], np.median(replicated), rtol=1e-4
            )


def test_runner_matches_legacy_on_aligned_polygons():
    """With polygons aligned to pixel edges, coverage fractions are
    whole pixels, so the weighted method must reproduce the legacy
    upsample-then-rasterize results exactly."""
    rng = np.random.default_rng(3)
    data = rng.random((2, 10, 10))
    dates = pd.date_range("2021-01-01", periods=2).strftime("%Y-%m-%d")
    da = make_da(data, coords={"date": dates})

    gdf = gpd.GeoDataFrame(
        {
            "geometry": [box(-5, -5, 0, 5), box(0, -5, 5, 0), box(0, 0, 5, 5)],
            "ADM1_PCODE": ["LEFT", "BOT", "TOP"],
        },
        crs="EPSG:4326",
    )

    da_up = upsample_raster(da)
    expected = fast_zonal_stats_runner(
        ds=da_up, gdf=gdf.copy(), adm_level=1, iso3="TST"
    )
    result = zonal_stats_runner(ds=da, gdf=gdf, adm_level=1, iso3="TST")

    result = result[expected.columns]
    for col in ["mean", "max", "min", "median", "sum", "std"]:
        np.testing.assert_allclose(
            result[col], expected[col], rtol=1e-6, err_msg=col
        )
    np.testing.assert_array_equal(result["count"], expected["count"])
    pd.testing.assert_frame_equal(
        result[["valid_date", "pcode", "adm_level", "iso3"]],
        expected[["valid_date", "pcode", "adm_level", "iso3"]],
    )


def test_runner_4d_leadtime_and_nan_skipping():
    rng = np.random.default_rng(5)
    data = rng.random((2, 2, 6, 6))
    data[1, 1] = np.nan  # invalid date/leadtime combo -> skipped
    da = make_da(
        data,
        dims=("date", "leadtime", "y", "x"),
        coords={"date": ["2021-01-01", "2021-02-01"], "leadtime": [0, 3]},
    )
    gdf = gpd.GeoDataFrame(
        {"geometry": [box(-5, -5, 5, 5)], "ADM2_PCODE": ["ALL"]},
        crs="EPSG:4326",
    )

    result = zonal_stats_runner(ds=da, gdf=gdf, adm_level=2, iso3="TST")

    # 2 dates x 2 leadtimes, minus the all-NaN combo
    assert len(result) == 3
    assert list(result["leadtime"]) == [0, 3, 0]
    assert list(result["issued_date"]) == [
        "2021-01-01",
        "2020-10-01",
        "2021-02-01",
    ]
    validate_stats_df(result, forecast=True)


def test_runner_admin_without_coverage():
    """Admins with no pixel coverage keep the legacy conventions:
    sum=0, count=0, other stats NaN."""
    data = np.ones((1, 6, 6))
    da = make_da(data, coords={"date": ["2021-01-01"]})
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [box(-5, -5, 5, 5), box(100, 100, 101, 101)],
            "ADM1_PCODE": ["IN", "OUT"],
        },
        crs="EPSG:4326",
    )

    result = zonal_stats_runner(ds=da, gdf=gdf, adm_level=1, iso3="TST")
    out_row = result[result["pcode"] == "OUT"].iloc[0]
    assert out_row["sum"] == 0.0
    assert out_row["count"] == 0
    for col in ["mean", "median", "min", "max", "std"]:
        assert np.isnan(out_row[col])


def test_subpixel_admin_keeps_count_at_least_one():
    """A polygon smaller than half a pixel still gets count >= 1, so a
    non-null mean always implies count > 0 (what the legacy method did,
    and what downstream `count > 0` filters assume)."""
    data = np.full((1, 6, 6), 5.0)
    da = make_da(data, coords={"date": ["2021-01-01"]})
    # ~1/25th of one 1-degree pixel, well under the rounding threshold
    # (the grid spans x -5..1, y -1..5)
    tiny = box(-4.9, 0.1, -4.7, 0.3)
    gdf = gpd.GeoDataFrame(
        {"geometry": [tiny], "ADM2_PCODE": ["TINY"]}, crs="EPSG:4326"
    )

    result = zonal_stats_runner(ds=da, gdf=gdf, adm_level=2, iso3="TST")
    row = result.iloc[0]
    assert row["mean"] == pytest.approx(5.0)
    assert row["count"] >= 1


def test_clip_raster_renames_floodscan_bands():
    data = np.ones((1, 2, 6, 6))
    da = make_da(
        data,
        dims=("date", "band", "y", "x"),
        coords={"date": ["2021-01-01"], "band": [1, 2]},
    )
    gdf = gpd.GeoDataFrame({"geometry": [box(-4, -4, 4, 4)]}, crs="EPSG:4326")
    clipped = clip_raster(da, gdf)
    assert list(clipped["band"].values) == ["SFED", "MFED"]


def test_weighted_median_integer_weights_matches_numpy():
    rng = np.random.default_rng(11)
    vals = rng.random((9, 4))
    w = rng.integers(1, 5, size=9).astype(float)
    expected = np.median(np.repeat(vals, w.astype(int), axis=0), axis=0)
    np.testing.assert_allclose(_weighted_median(vals, w), expected)


def test_weighted_median_handles_nan_and_empty():
    vals = np.array([[1.0, np.nan], [3.0, np.nan], [10.0, np.nan]])
    w = np.array([1.0, 1.0, 1.0])
    result = _weighted_median(vals, w)
    assert result[0] == 3.0
    assert np.isnan(result[1])


def test_weights_cache_roundtrip(random_gdf):
    data = np.zeros((1, 10, 10))
    da = make_da(data, coords={"date": ["2021-01-01"]})
    W1 = zonal_utils.load_or_build_weights(
        random_gdf, "ADM1_PCODE", "TST", 1, da
    )
    # Second call hits the on-disk cache
    W2 = zonal_utils.load_or_build_weights(
        random_gdf, "ADM1_PCODE", "TST", 1, da
    )
    assert (W1 != W2).nnz == 0


def test_validate_stats_df_raises():
    df = pd.DataFrame(
        {
            "mean": [5.0],
            "max": [4.0],  # mean > max
            "min": [1.0],
            "median": [2.0],
            "sum": [10.0],
            "std": [1.0],
            "count": [3],
            "valid_date": ["2021-01-01"],
            "pcode": ["X"],
            "adm_level": [1],
            "iso3": ["TST"],
        }
    )
    with pytest.raises(ValueError, match="mean outside"):
        validate_stats_df(df, forecast=False)
