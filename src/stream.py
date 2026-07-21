"""
Stream the given remote COPC with HTTP Range Requests.

Fetch only the LAS header and COPC octree index via HTTP Range Requests, then pull point data.

This script proves bounded memory: one tile at a time, discard points, log RSS.
"""

import logging
import time

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


def main() -> None:
    peak = rss_mb()
    total_points = 0
    tiles = 0
    t0 = time.perf_counter()

    log.info("Opening %s (RSS %.1f MB)", URL, peak)

    # Opening reads header + COPC index only (small Range Requests).
    with CopcReader.open(URL) as reader:
        # Scaled world bounds from the LAS header
        xmin, ymin, zmin = reader.header.mins
        xmax, ymax, zmax = reader.header.maxs
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
            total_points += n

            # Drop the tile before the next query so peak RAM stays bounded.
            del points

            tiles += 1
            mem = rss_mb()
            peak = max(peak, mem)
            log.info(
                "tile %d | +%d pts (total %d) | RSS %.1f MB (peak %.1f)",
                tiles, n, total_points, mem, peak,
            )

    log.info(
        "Done in %.1fs | tiles=%d points=%d peak_RSS=%.1f MB",
        time.perf_counter() - t0, tiles, total_points, peak,
    )
    # Good sign: if peak RSS stays far below ~2 GB file size.
    log.info("If peak RSS stays far below ~2 GB, streaming is OK.")


if __name__ == "__main__":
    main()