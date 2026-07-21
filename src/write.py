"""
Bulk-write enriched voxels to GeoParquet (EPSG:32611).

Usage (from src/):
  python write.py
"""

import logging
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

log = logging.getLogger("storage")

# SoFi COPC CRS from header.parse_crs()
CRS = "EPSG:32611"
DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent / "data" / "sofi_voxels.parquet"

Row = Mapping[str, Union[str, int, float]]


def rows_to_geodataframe(rows: Iterable[Row], crs: str = CRS) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame; geometry = voxel center (x, y) with z_mean as Z."""
    records = list(rows)
    if not records:
        raise ValueError("No rows to write")

    df = pd.DataFrame.from_records(records)
    geometry = [
        Point(float(r["voxel_x"]), float(r["voxel_y"]), float(r["z_mean"]))
        for r in records
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    # Sort for better Parquet row-group pruning on spatial filters
    return gdf.sort_values(["voxel_x", "voxel_y", "voxel_z"]).reset_index(drop=True)


def write_geoparquet(
    rows: Iterable[Row],
    output_path: Union[str, Path] = DEFAULT_OUTPUT,
    crs: str = CRS,
) -> Path:
    """Write enriched voxel rows to a GeoParquet file (bulk write, not row-by-row)."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    gdf = rows_to_geodataframe(rows, crs=crs)
    gdf.to_parquet(path, index=False)

    log.info(
        "Wrote %d voxels → %s (crs=%s, %.1f KB)",
        len(gdf),
        path,
        crs,
        path.stat().st_size / 1024,
    )
    return path


def verify_geoparquet(path: Union[str, Path] = DEFAULT_OUTPUT) -> gpd.GeoDataFrame:
    """Reload and print a short summary for README verification."""
    path = Path(path)
    gdf = gpd.read_parquet(path)
    log.info("Verified %s | rows=%d crs=%s columns=%s", path, len(gdf), gdf.crs, list(gdf.columns))
    log.info("Sample:\n%s", gdf.head(3).to_string())
    return gdf


def main(rows: Optional[List[Row]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if rows is None:
        from enrich import enrich

        log.info("No rows passed — running enrich() then writing GeoParquet")
        rows = enrich()

    out = write_geoparquet(rows)
    verify_geoparquet(out)


if __name__ == "__main__":
    main()
