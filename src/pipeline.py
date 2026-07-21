"""
Prefect orchestration for the SoFi COPC pipeline.

Tasks: validate_source → enrich_voxels → save_geoparquet → verify_output

Run (from src/):
  python pipeline.py
  python pipeline.py --max-tiles 3
"""

import time
from pathlib import Path
from typing import Dict, List, Optional

from laspy import CopcReader
from prefect import flow, task
from prefect.logging import get_run_logger

from enrich import enrich
from stream import TILE_SIZE, URL, iter_xy_tiles
from write import CRS, DEFAULT_OUTPUT, verify_geoparquet, write_geoparquet


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _count_tiles(mins, maxs, tile_size: float = TILE_SIZE) -> int:
    xmin, ymin, _ = mins
    xmax, ymax, _ = maxs
    return sum(1 for _ in iter_xy_tiles(xmin, ymin, xmax, ymax, tile_size))


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def validate_source(url: str = URL, max_tiles: Optional[int] = None) -> dict:
    """Fail fast: open remote COPC and log points/tiles planned for this run."""
    logger = get_run_logger()
    t0 = time.perf_counter()
    with CopcReader.open(url) as reader:
        crs = reader.header.parse_crs()
        mins = [float(v) for v in reader.header.mins]
        maxs = [float(v) for v in reader.header.maxs]
        header_point_count = int(reader.header.point_count)
        tiles_in_extent = _count_tiles(mins, maxs)
        tiles_to_process = (
            tiles_in_extent if max_tiles is None else min(tiles_in_extent, max_tiles)
        )
        info = {
            "url": url,
            "header_point_count": header_point_count,
            "mins": mins,
            "maxs": maxs,
            "crs": str(crs) if crs else None,
            "tiles_in_extent": tiles_in_extent,
            "tiles_to_process": tiles_to_process,
            "max_tiles": max_tiles,
            "full_extent_requested": max_tiles is None,
        }

    logger.info("Source OK in %s", _format_duration(time.perf_counter() - t0))
    logger.info(
        "TO PROCESS | points_in_file=%s | tiles_in_extent=%d | tiles_this_run=%d | full_extent=%s",
        f"{header_point_count:,}",
        tiles_in_extent,
        tiles_to_process,
        max_tiles is None,
    )
    return info


@task(retries=1, retry_delay_seconds=30, log_prints=True)
def enrich_voxels(url: str = URL, max_tiles: Optional[int] = None) -> Dict:
    """Stream tiles and build voxel metrics. Returns {rows, stats}."""
    logger = get_run_logger()
    t0 = time.perf_counter()
    logger.info("Enrich starting (tile-level ETA logged per tile)")
    result = enrich(url=url, max_tiles=max_tiles)
    rows = result["rows"]
    stats = result["stats"]
    if not rows:
        raise ValueError("Enrich produced 0 voxels")

    logger.info(
        "Enrich task finished in %s | voxels=%d",
        _format_duration(time.perf_counter() - t0),
        len(rows),
    )
    logger.info(
        "PROCESSED | tiles=%d/%d (extent had %d) | points_queried=%s / header=%s",
        stats["tiles_processed"],
        stats["tiles_planned"],
        stats["tiles_in_extent"],
        f"{stats['points_queried']:,}",
        f"{stats['header_point_count']:,}",
    )
    logger.info(
        "COMPLETE? | all_tiles_processed=%s | all_points_processed=%s "
        "(full_extent=%s, full_resolution=%s)",
        stats["all_tiles_processed"],
        stats["all_points_processed"],
        stats["full_extent_requested"],
        stats["full_resolution_requested"],
    )
    if not stats["all_tiles_processed"]:
        logger.warning("Not all planned tiles were processed.")
    if not stats["all_points_processed"]:
        logger.warning(
            "Not a full-point pass (tile limit and/or resolution LOD still active, "
            "or tile loop incomplete)."
        )
    else:
        logger.info("Full-extent, full-resolution tile pass completed.")

    return result


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def save_geoparquet(rows: list, output_path: str) -> str:
    logger = get_run_logger()
    t0 = time.perf_counter()
    path = write_geoparquet(rows, output_path=Path(output_path), crs=CRS)
    logger.info("Write finished in %s → %s", _format_duration(time.perf_counter() - t0), path)
    return str(path)


@task(log_prints=True)
def verify_output(path: str) -> int:
    logger = get_run_logger()
    t0 = time.perf_counter()
    gdf = verify_geoparquet(path)
    logger.info("Verify finished in %s | rows=%d", _format_duration(time.perf_counter() - t0), len(gdf))
    return len(gdf)


@flow(name="sofi-copc-pipeline", log_prints=True)
def sofi_pipeline(
    url: str = URL,
    output_path: str = str(DEFAULT_OUTPUT),
    max_tiles: Optional[int] = None,
):
    """
    End-to-end pipeline.

    Prefect records per-task duration in the UI/logs automatically.
    """
    logger = get_run_logger()
    flow_t0 = time.perf_counter()
    logger.info("Flow start | url=%s max_tiles=%s output=%s", url, max_tiles, output_path)

    source = validate_source(url, max_tiles=max_tiles)
    result = enrich_voxels(url, max_tiles=max_tiles)
    rows: List[dict] = result["rows"]
    stats = result["stats"]

    if not rows:
        logger.warning("No voxels produced — skipping write.")
        return None

    path = save_geoparquet(rows, output_path)
    n = verify_output(path)

    logger.info(
        "Flow complete in %s | %d voxels → %s",
        _format_duration(time.perf_counter() - flow_t0),
        n,
        path,
    )
    logger.info(
        "FINAL COVERAGE | planned_tiles=%d processed_tiles=%d | "
        "header_points=%s points_queried=%s | all_tiles=%s all_points=%s",
        source["tiles_to_process"],
        stats["tiles_processed"],
        f"{stats['header_point_count']:,}",
        f"{stats['points_queried']:,}",
        stats["all_tiles_processed"],
        stats["all_points_processed"],
    )
    return path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SoFi COPC Prefect pipeline")
    parser.add_argument("--max-tiles", type=int, default=None, metavar="N")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    sofi_pipeline(max_tiles=args.max_tiles, output_path=args.output)
