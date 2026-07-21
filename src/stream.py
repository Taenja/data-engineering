"""
Stream the given remote COPC with HTTP Range Requests.

Fetch only the LAS header and COPC octree index via HTTP Range Requests, then pull point data.

This script proves bounded memory: one tile at a time, discard points, log RSS.

Usage (from src/):
  python stream.py
  python stream.py --max-nonempty-tiles 3  # do this step only to test data streaming and get subset of data
"""

import argparse
import logging
import time
from collections import Counter
from typing import Optional

import numpy as np
import psutil
from laspy import Bounds, CopcReader

# Public SoFi Stadium COPC on S3
URL = "https://s3.amazonaws.com/hobu-lidar/sofi.copc.laz"

# Tile size in CRS units (meters for EPSG:32611 UTM, found in eda.py).
# Smaller tiles = lower peak RAM per query; more HTTP round-trips.
# Larger tiles = fewer queries; more points held in memory at once.
TILE_SIZE = 100.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("streaming")


def rss_mb() -> float:
    """Current process resident set size in MB (for memory-bounded proof)."""
    return psutil.Process().memory_info().rss / (1024 * 1024)


def iter_xy_tiles(xmin, ymin, xmax, ymax, tile_size):
    """
    Yield XY bounding boxes that cover the full extent.

    Each yield is (x0, y0, x1, y1) in projected coordinates.
    Tiles are not saved anywhere — they are just query windows in memory.
    """
    x = xmin
    while x < xmax:
        x1 = min(x + tile_size, xmax)
        y = ymin
        while y < ymax:
            y1 = min(y + tile_size, ymax)
            yield x, y, x1, y1
            y = y1
        x = x1


def log_tile_inspection(
    points,
    nonempty_idx: int,
    grid_idx: int,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    dimension_names: list[str],
) -> None:
    """Log per-tile stats useful for spot-checking streamed COPC data."""
    n = len(points)
    log.info(
        "--- nonempty tile %d (grid tile %d) | bounds X[%.1f, %.1f] Y[%.1f, %.1f] | %d pts ---",
        nonempty_idx, grid_idx, x0, x1, y0, y1, n,
    )
    for axis in ("X", "Y", "Z"):
        if axis in dimension_names:
            vals = np.asarray(getattr(points, axis.lower()))
            log.info(
                "  %s: min=%.2f max=%.2f mean=%.2f",
                axis.upper(), vals.min(), vals.max(), vals.mean(),
            )
    if "intensity" in dimension_names:
        intensity = np.asarray(points.intensity)
        log.info(
            "  intensity: min=%d max=%d mean=%.1f",
            intensity.min(), intensity.max(), intensity.mean(),
        )
    if "classification" in dimension_names:
        uniq, counts = np.unique(points.classification, return_counts=True)
        breakdown = ", ".join(f"{c}:{cnt}" for c, cnt in zip(uniq, counts))
        log.info("  classification: %s", breakdown)
    if "return_number" in dimension_names:
        uniq, counts = np.unique(points.return_number, return_counts=True)
        breakdown = ", ".join(f"{r}:{cnt}" for r, cnt in zip(uniq, counts))
        log.info("  return_number: %s", breakdown)


def main(max_nonempty_tiles: Optional[int] = None) -> None:
    inspect = max_nonempty_tiles is not None
    peak = rss_mb()
    total_points = 0
    tiles_scanned = 0
    nonempty_tiles = 0
    class_counts: Counter[int] = Counter()
    t0 = time.perf_counter()

    log.info("Opening %s (RSS %.1f MB)", URL, peak)
    if inspect:
        log.info("Inspect mode: stop after %d nonempty tiles", max_nonempty_tiles)

    # Opening reads header + COPC index only (small Range Requests).
    with CopcReader.open(URL) as reader:
        hdr = reader.header
        dimension_names = list(hdr.point_format.dimension_names)
        if inspect:
            log.info("CRS: %s", hdr.parse_crs())
            log.info("Dimensions: %s", dimension_names)

        # Scaled world bounds from the LAS header
        xmin, ymin, zmin = hdr.mins
        xmax, ymax, zmax = hdr.maxs
        log.info(
            "Bounds X[%.1f, %.1f] Y[%.1f, %.1f] Z[%.1f, %.1f] | RSS %.1f MB",
            xmin, xmax, ymin, ymax, zmin, zmax, rss_mb(),
        )

        # Walk the XY grid; Z spans full vertical extent per tile.
        for x0, y0, x1, y1 in iter_xy_tiles(xmin, ymin, xmax, ymax, TILE_SIZE):
            bounds = Bounds(
                mins=np.array([x0, y0, zmin]),
                maxs=np.array([x1, y1, zmax]),
            )

            # laspy fetches only octree nodes overlapping this box (Range Requests).
            points = reader.query(bounds=bounds)
            n = len(points)
            tiles_scanned += 1
            mem = rss_mb()
            peak = max(peak, mem)

            if inspect:
                if n == 0:
                    log.info(
                        "grid tile %d | empty | RSS %.1f MB (peak %.1f)",
                        tiles_scanned, mem, peak,
                    )
                else:
                    nonempty_tiles += 1
                    total_points += n
                    log_tile_inspection(
                        points, nonempty_tiles, tiles_scanned, x0, y0, x1, y1, dimension_names,
                    )
                    if "classification" in dimension_names:
                        uniq, counts = np.unique(points.classification, return_counts=True)
                        class_counts.update(dict(zip(uniq.tolist(), counts.tolist())))
                    log.info(
                        "grid tile %d | +%d pts (nonempty total %d pts) | RSS %.1f MB (peak %.1f)",
                        tiles_scanned, n, total_points, mem, peak,
                    )
            else:
                total_points += n
                log.info(
                    "tile %d | +%d pts (total %d) | RSS %.1f MB (peak %.1f)",
                    tiles_scanned, n, total_points, mem, peak,
                )

            # Drop the tile before the next query so peak RAM stays bounded.
            del points

            if inspect and nonempty_tiles >= max_nonempty_tiles:
                break

    if inspect:
        log.info(
            "Done in %.1fs | grid_tiles=%d nonempty_tiles=%d points=%d peak_RSS=%.1f MB",
            time.perf_counter() - t0, tiles_scanned, nonempty_tiles, total_points, peak,
        )
        if class_counts:
            log.info("Classification totals (nonempty tiles): %s", dict(class_counts))
    else:
        log.info(
            "Done in %.1fs | tiles=%d points=%d peak_RSS=%.1f MB",
            time.perf_counter() - t0, tiles_scanned, total_points, peak,
        )
    # Good sign: if peak RSS stays far below ~2 GB file size.
    log.info("If peak RSS stays far below ~2 GB, streaming is OK.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream remote COPC tiles and log RSS")
    parser.add_argument(
        "--max-nonempty-tiles",
        type=int,
        default=None,
        metavar="N",
        help="inspect data and stop after N tiles that contain points (default: stream full extent)",
    )
    args = parser.parse_args()
    main(max_nonempty_tiles=args.max_nonempty_tiles)