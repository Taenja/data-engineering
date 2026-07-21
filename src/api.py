"""
Lightweight FastAPI consumer for enriched SoFi voxel GeoParquet.

Serves summary stats, filtered queries, and a small HTML viewer.

Run (from src/):
  uvicorn api:app --reload --port 8000

Then open http://127.0.0.1:8000/

to use a different file - VOXEL_PARQUET=/path/to/other.parquet uvicorn api:app --reload --port 8000
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import geopandas as gpd
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from write import DEFAULT_OUTPUT

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
PARQUET_PATH = Path(os.environ.get("VOXEL_PARQUET", str(DEFAULT_OUTPUT)))

# ASPRS LAS classification labels (subset used in LiDAR viewers)
ASPRS_CLASS_NAMES = {
    0: "Never classified",
    1: "Unclassified",
    2: "Ground",
    3: "Low vegetation",
    4: "Medium vegetation",
    5: "High vegetation",
    6: "Building",
    7: "Low point (noise)",
    9: "Water",
    17: "Bridge deck",
}

app = FastAPI(
    title="SoFi Voxel API",
    description="Query enriched voxel metrics from the COPC pipeline GeoParquet output.",
    version="1.0.0",
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@lru_cache(maxsize=1)
def load_voxels(path: str = str(PARQUET_PATH)) -> gpd.GeoDataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Parquet not found: {p}. Run the pipeline first (python pipeline.py)."
        )
    return gpd.read_parquet(p)


def _df_records(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
    out = df.head(limit).copy()
    if "geometry" in out.columns:
        out = out.drop(columns=["geometry"])
    return out.to_dict(orient="records")


@app.get("/", response_class=HTMLResponse)
def home() -> FileResponse:
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(404, "static/index.html missing")
    return FileResponse(index)


@app.get("/health")
def health() -> dict:
    try:
        gdf = load_voxels()
        return {"status": "ok", "rows": len(gdf), "path": str(PARQUET_PATH)}
    except FileNotFoundError as e:
        return {"status": "missing_data", "detail": str(e)}


@app.get("/stats")
def stats() -> dict:
    try:
        gdf = load_voxels()
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e

    numeric = gdf.select_dtypes(include="number")
    summary = {}
    for col in [
        "point_count",
        "point_density",
        "intensity_mean",
        "z_mean",
        "z_std",
        "z_min",
        "z_max",
    ]:
        if col in numeric.columns:
            s = numeric[col]
            summary[col] = {
                "min": float(s.min()),
                "max": float(s.max()),
                "mean": float(s.mean()),
                "std": float(s.std(ddof=0)),
            }

    class_counts: Dict[str, int] = {}
    if "classification_mode" in gdf.columns:
        for code, count in gdf["classification_mode"].value_counts().items():
            label = ASPRS_CLASS_NAMES.get(int(code), f"Class {int(code)}")
            class_counts[f"{int(code)} — {label}"] = int(count)

    return {
        "path": str(PARQUET_PATH.resolve()),
        "rows": len(gdf),
        "crs": gdf.crs.to_string() if gdf.crs is not None else None,
        "epsg": gdf.crs.to_epsg() if gdf.crs is not None else None,
        "columns": list(gdf.columns),
        "bounds": {
            "voxel_x": [float(gdf["voxel_x"].min()), float(gdf["voxel_x"].max())],
            "voxel_y": [float(gdf["voxel_y"].min()), float(gdf["voxel_y"].max())],
            "z_mean": [float(gdf["z_mean"].min()), float(gdf["z_mean"].max())],
        },
        "metrics": summary,
        "classification_mode": class_counts,
        "asprs_labels": ASPRS_CLASS_NAMES,
    }


@app.get("/voxels")
def list_voxels(
    min_x: Optional[float] = Query(None, description="Min voxel_x (UTM easting)"),
    max_x: Optional[float] = Query(None),
    min_y: Optional[float] = Query(None, description="Min voxel_y (UTM northing)"),
    max_y: Optional[float] = Query(None),
    min_density: Optional[float] = Query(None, ge=0),
    min_z: Optional[float] = Query(None),
    max_z: Optional[float] = Query(None),
    sort_by: str = Query("point_count", description="Column to sort by"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=5000),
) -> dict:
    """Filter / sort enriched voxels — typical analyst query pattern."""
    try:
        gdf = load_voxels()
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e

    df = gdf
    if min_x is not None:
        df = df[df["voxel_x"] >= min_x]
    if max_x is not None:
        df = df[df["voxel_x"] <= max_x]
    if min_y is not None:
        df = df[df["voxel_y"] >= min_y]
    if max_y is not None:
        df = df[df["voxel_y"] <= max_y]
    if min_density is not None:
        df = df[df["point_density"] >= min_density]
    if min_z is not None:
        df = df[df["z_mean"] >= min_z]
    if max_z is not None:
        df = df[df["z_mean"] <= max_z]

    if sort_by not in df.columns:
        raise HTTPException(400, f"Unknown sort_by column: {sort_by}")

    df = df.sort_values(sort_by, ascending=(order == "asc"))
    records = _df_records(df, limit)

    return {
        "matched": int(len(df)),
        "returned": len(records),
        "sort_by": sort_by,
        "order": order,
        "voxels": records,
    }


@app.get("/voxels/{voxel_id}")
def get_voxel(voxel_id: str) -> dict:
    try:
        gdf = load_voxels()
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e

    hit = gdf[gdf["voxel_id"] == voxel_id]
    if hit.empty:
        raise HTTPException(404, f"voxel_id not found: {voxel_id}")

    row = hit.drop(columns=["geometry"], errors="ignore").iloc[0].to_dict()
    for k, v in list(row.items()):
        if hasattr(v, "item"):
            row[k] = v.item()
    return row


@app.get("/viz/sample")
def viz_sample(
    limit: int = Query(8000, ge=100, le=50000),
    color_by: str = Query("intensity_mean"),
) -> dict:
    """Downsampled voxels in WGS84 for the Leaflet world-map viewer."""
    try:
        gdf = load_voxels()
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e

    if color_by not in gdf.columns:
        raise HTTPException(400, f"Unknown color_by: {color_by}")

    if len(gdf) > limit:
        sample = gdf.sample(n=limit, random_state=42).copy()
    else:
        sample = gdf.copy()

    if sample.crs is None:
        sample = sample.set_crs(32611)
    wgs84 = sample.to_crs(epsg=4326)
    lon = wgs84.geometry.x.astype(float)
    lat = wgs84.geometry.y.astype(float)

    is_categorical = color_by == "classification_mode"
    if is_categorical:
        color_values = sample[color_by].astype(int).tolist()
    else:
        color_values = sample[color_by].astype(float).tolist()

    class_counts: Dict[str, int] = {}
    if "classification_mode" in sample.columns:
        for code, count in sample["classification_mode"].value_counts().items():
            label = ASPRS_CLASS_NAMES.get(int(code), f"Class {int(code)}")
            class_counts[str(int(code))] = {
                "count": int(count),
                "label": label,
            }

    return {
        "count": len(sample),
        "color_by": color_by,
        "is_categorical": is_categorical,
        "crs_source": "EPSG:32611",
        "crs_map": "EPSG:4326",
        "lon": lon.tolist(),
        "lat": lat.tolist(),
        "x": sample["voxel_x"].astype(float).tolist(),
        "y": sample["voxel_y"].astype(float).tolist(),
        "z": sample["z_mean"].astype(float).tolist(),
        "color": color_values,
        "class_counts": class_counts,
        "intensity_mean": sample["intensity_mean"].astype(float).tolist(),
        "point_count": sample["point_count"].astype(int).tolist(),
        "point_density": sample["point_density"].astype(float).tolist(),
        "classification_mode": sample["classification_mode"].astype(int).tolist(),
        "voxel_id": sample["voxel_id"].astype(str).tolist(),
        "center": {"lon": float(lon.mean()), "lat": float(lat.mean())},
        "bounds": {
            "west": float(lon.min()),
            "south": float(lat.min()),
            "east": float(lon.max()),
            "north": float(lat.max()),
        },
    }


@app.post("/reload")
def reload_data() -> dict:
    """Clear cache after re-running the pipeline."""
    load_voxels.cache_clear()
    gdf = load_voxels()
    return {"reloaded": True, "rows": len(gdf)}
