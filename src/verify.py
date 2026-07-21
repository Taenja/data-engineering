"""
Explore and verify enriched GeoParquet output — clean tables for review / demos.

Usage (from src/):
  python verify.py
  python verify.py /path/to/sofi_voxels.parquet
  python verify.py --rows 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from write import DEFAULT_OUTPUT

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 140)
pd.set_option("display.max_colwidth", 40)
pd.set_option("display.float_format", lambda x: f"{x:,.4f}")

REQUIRED_COLUMNS = {
    "voxel_id",
    "voxel_x",
    "voxel_y",
    "point_count",
    "point_density",
    "intensity_mean",
    "z_mean",
    "classification_mode",
    "geometry",
}

# Enrichment metrics only — exclude grid indices and UTM origins (see Spatial bounds).
METRIC_COLUMNS = [
    "point_count",
    "point_density",
    "intensity_mean",
    "z_mean",
    "z_std",
    "z_min",
    "z_max",
    "classification_mode",
]


def _banner(title: str) -> None:
    line = "=" * 72
    print(f"\n{line}\n  {title}\n{line}")


def explore(path: Path, sample_rows: int = 15) -> None:
    if not path.exists():
        print(f"ERROR: file not found: {path}", file=sys.stderr)
        print("Run the pipeline first: python pipeline.py", file=sys.stderr)
        sys.exit(1)

    gdf = gpd.read_parquet(path)

    _banner("File")
    crs_label = gdf.crs.to_string() if gdf.crs is not None else "None"
    if gdf.crs is not None and gdf.crs.to_epsg() is not None:
        crs_label = f"EPSG:{gdf.crs.to_epsg()}"

    print(f"  path        : {path.resolve()}")
    print(f"  size        : {path.stat().st_size / 1024:,.1f} KB")
    print(f"  rows        : {len(gdf):,}")
    print(f"  columns     : {len(gdf.columns)}")
    print(f"  CRS         : {crs_label}")
    print(f"  geometry    : {gdf.geometry.geom_type.value_counts().to_dict()}")

    missing = REQUIRED_COLUMNS - set(gdf.columns)
    if missing:
        print(f"  WARNING     : missing expected columns: {sorted(missing)}")
    else:
        print("  schema OK   : all expected enrichment columns present")

    _banner("Schema (dtypes)")
    schema = pd.DataFrame(
        {
            "column": gdf.columns,
            "dtype": [str(t) for t in gdf.dtypes],
            "non_null": [int(gdf[c].notna().sum()) for c in gdf.columns],
        }
    )
    print(schema.to_string(index=False))

    value_cols = [c for c in gdf.columns if c != "geometry"]
    sample = gdf[value_cols].head(sample_rows).copy()

    _banner(f"Sample rows (first {len(sample)})")
    print(sample.to_string(index=True))

    _banner("Geometry sample (WKT)")
    geom_preview = gdf.geometry.head(min(5, len(gdf))).apply(lambda g: g.wkt)
    for i, wkt in geom_preview.items():
        print(f"  [{i}] {wkt}")

    metric_cols = [c for c in METRIC_COLUMNS if c in gdf.columns]
    if metric_cols:
        _banner("Enrichment metrics (summary)")
        summary = gdf[metric_cols].describe().T
        summary = summary[["count", "mean", "std", "min", "50%", "max"]]
        summary = summary.rename(columns={"50%": "median"})
        print(summary.to_string())

    if "classification_mode" in gdf.columns:
        _banner("Classification mode (top ASPRS classes)")
        counts = gdf["classification_mode"].value_counts().head(10)
        for code, count in counts.items():
            print(f"  class {int(code):2d}: {count:,} voxels")

    if {"voxel_x", "voxel_y", "z_mean"}.issubset(gdf.columns):
        _banner("Spatial bounds")
        print(f"  voxel_x : {gdf['voxel_x'].min():,.2f} → {gdf['voxel_x'].max():,.2f}")
        print(f"  voxel_y : {gdf['voxel_y'].min():,.2f} → {gdf['voxel_y'].max():,.2f}")
        print(f"  z_mean  : {gdf['z_mean'].min():,.2f} → {gdf['z_mean'].max():,.2f}")

    _banner("Top voxels by point_count")
    if "point_count" in gdf.columns:
        top = gdf[value_cols].nlargest(10, "point_count").reset_index(drop=True)
        print(top.to_string(index=True))
    else:
        print("  (point_count column not found)")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify SoFi voxel GeoParquet output")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(DEFAULT_OUTPUT),
        help=f"Parquet path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=15,
        help="Number of sample rows to print (default: 15)",
    )
    args = parser.parse_args()
    explore(Path(args.path), sample_rows=args.rows)


if __name__ == "__main__":
    main()
