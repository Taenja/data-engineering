"""
Enrich streamed COPC tiles: voxel-grid downsampling + per-voxel metrics.

Streams tiles from remote COPC (HTTP Range Requests), bins points into 5 m voxels,
and keeps only running aggregates in memory — not the full point cloud.

Per-voxel output metrics:
  - Elevation: z_mean, z_std, z_min, z_max
  - Density: point_count, point_density (points per m³)
  - Intensity: intensity_mean
  - ASPRS classification: classification_mode (most common class in the voxel)

Usage (from src/):
  python data_enrich.py
"""


import logging
import time
from typing import Dict, List, Tuple, Optional

import numpy as np
from laspy import Bounds, CopcReader

from stream import URL, TILE_SIZE, iter_xy_tiles, rss_mb

# --- Config ------------------------------------------------------------------

# Voxel edge length in CRS units (meters for EPSG:32611).
# Each voxel is a 5×5×5 m cube; all points inside are summarized into one row.
VOXEL_SIZE = 5.0

# COPC level-of-detail passed to reader.query():
#   None  = densest available points in each tile (slowest, most complete)
#   2.0   = coarser octree level (faster smoke tests, fewer points per tile)
RESOLUTION: Optional[float] = None

N_CLASSES = 32  # histogram bins cover ASPRS codes 0–31

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich")

# --- In-memory aggregate types -------------------------------------------------

VoxelKey = Tuple[int, int, int]  # (ix, iy, iz) grid indices

# Per-voxel numeric running totals (updated tile-by-tile, never stores raw points):
#   [0] point count
#   [1] z_sum          — for z_mean
#   [2] z_sum_sq       — for z_std (population variance)
#   [3] z_min
#   [4] z_max
#   [5] intensity_sum  — for intensity_mean
Voxels = Dict[VoxelKey, np.ndarray]

# Per-voxel ASPRS class histogram (32 bins). Used for classification_mode.
ClassHist = Dict[VoxelKey, np.ndarray]


def _empty_stats() -> np.ndarray:
    return np.array([0.0, 0.0, 0.0, np.inf, -np.inf, 0.0], dtype=np.float64)


def _merge_class_hist(key: VoxelKey, cls_chunk: np.ndarray, class_hist: ClassHist) -> None:
    """
    Add classification counts from one voxel's point chunk into its histogram.

    Uses np.bincount — O(n) per chunk, no Python loop over individual points.
    """
    if cls_chunk.size == 0:
        return
    bc = np.bincount(cls_chunk.astype(np.int64), minlength=N_CLASSES)
    if key not in class_hist:
        class_hist[key] = np.zeros(N_CLASSES, dtype=np.int64)
    class_hist[key] += bc[:N_CLASSES]


def accumulate_points(
    points,
    voxel_size: float,
    voxels: Voxels,
    class_hist: ClassHist,
) -> int:
    """
    Fold one tile's points into the global voxel aggregate dicts.

    Points are binned by floor(coord / voxel_size), sorted by voxel key,
    then summarized per contiguous group. Raw arrays are not retained.
    Returns the number of points processed in this tile.
    """
    n = len(points)
    if n == 0:
        return 0

    # Scaled world coordinates (meters) — not raw LAS integer X/Y/Z.
    x = np.asarray(points.x, dtype=np.float64)
    y = np.asarray(points.y, dtype=np.float64)
    z = np.asarray(points.z, dtype=np.float64)
    intensity = np.asarray(points.intensity, dtype=np.float64)
    classification = np.asarray(points.classification, dtype=np.int64)

    # Convert position → integer grid cell (ix, iy, iz).
    ix = np.floor(x / voxel_size).astype(np.int64)
    iy = np.floor(y / voxel_size).astype(np.int64)
    iz = np.floor(z / voxel_size).astype(np.int64)

    keys = np.stack([ix, iy, iz], axis=1)

    # Sort by (ix, iy, iz) so all points sharing a voxel are adjacent.
    order = np.lexsort((keys[:, 2], keys[:, 1], keys[:, 0]))
    keys = keys[order]
    z = z[order]
    intensity = intensity[order]
    classification = classification[order]

    # Find slice boundaries where the voxel identity changes.
    changed = np.any(keys[1:] != keys[:-1], axis=1)
    starts = np.concatenate(([0], np.nonzero(changed)[0] + 1))
    ends = np.concatenate((starts[1:], [len(keys)]))

    for start, end in zip(starts, ends):
        key = (int(keys[start, 0]), int(keys[start, 1]), int(keys[start, 2]))
        z_chunk = z[start:end]
        i_chunk = intensity[start:end]
        c_chunk = classification[start:end]

        stats = voxels.get(key)
        if stats is None:
            stats = _empty_stats()
            voxels[key] = stats

        # Update running elevation + intensity stats for this voxel.
        stats[0] += z_chunk.size
        stats[1] += z_chunk.sum()
        stats[2] += np.square(z_chunk).sum()
        stats[3] = min(stats[3], z_chunk.min())
        stats[4] = max(stats[4], z_chunk.max())
        stats[5] += i_chunk.sum()

        _merge_class_hist(key, c_chunk, class_hist)

    return n


def _classification_row(key: VoxelKey, class_hist: ClassHist) -> dict:
    """Return the most common ASPRS class in the voxel."""
    hist = class_hist.get(key)
    if hist is None or hist.sum() == 0:
        return {"classification_mode": -1}
    return {"classification_mode": int(np.argmax(hist))}


def voxels_to_rows(
    voxels: Voxels,
    class_hist: ClassHist,
    voxel_size: float,
) -> List[dict]:
    """
    Convert in-memory aggregates to flat dict rows for GeoParquet export.

    Called once after all tiles are processed — the only time we materialize
    the full output list (still far smaller than raw points).
    """
    voxel_volume = voxel_size ** 3
    rows = []
    for (ix, iy, iz), s in voxels.items():
        count = int(s[0])
        mean = s[1] / count
        var = max(s[2] / count - mean * mean, 0.0)
        key = (ix, iy, iz)

        row = {
            "voxel_id": f"{ix}:{iy}:{iz}",
            "voxel_ix": ix,
            "voxel_iy": iy,
            "voxel_iz": iz,
            "voxel_x": ix * voxel_size,
            "voxel_y": iy * voxel_size,
            "voxel_z": iz * voxel_size,
            "point_count": count,
            "point_density": float(count / voxel_volume),
            "intensity_mean": float(s[5] / count),
            "z_mean": float(mean),
            "z_std": float(var**0.5),
            "z_min": float(s[3]),
            "z_max": float(s[4]),
        }
        row.update(_classification_row(key, class_hist))
        rows.append(row)
    return rows

def enrich(
    url: str = URL,
    tile_size: float = TILE_SIZE,
    voxel_size: float = VOXEL_SIZE,
    resolution: Optional[float] = RESOLUTION,
) -> List[dict]:
    """
    Main enrichment loop: stream COPC tiles → accumulate voxels → return rows.
    """

    voxels: Voxels = {}
    class_hist: ClassHist = {}
    total_points = 0
    tiles = 0
    peak = rss_mb()
    t0 = time.perf_counter()

    log.info(
        "Enrich start | voxel=%.1f tile=%.1f resolution=%s",
        voxel_size,
        tile_size,
        resolution,
    )
    log.info("RSS before open: %.1f MB", peak)

    with CopcReader.open(url) as reader:
        xmin, ymin, zmin = reader.header.mins
        xmax, ymax, zmax = reader.header.maxs

        total_tiles = sum(1 for _ in iter_xy_tiles(xmin, ymin, xmax, ymax, tile_size))
        log.info("Total tiles to process: %d", total_tiles)

        for x0, y0, x1, y1 in iter_xy_tiles(xmin, ymin, xmax, ymax, tile_size):
            # Spatial query window: this XY tile, full Z extent of the cloud.
            bounds = Bounds(
                mins=np.array([x0, y0, zmin]),
                maxs=np.array([x1, y1, zmax]),
            )
            kwargs = {"bounds": bounds}
            if resolution is not None:
                kwargs["resolution"] = resolution  # COPC LOD thinning

            points = reader.query(**kwargs)
            n = accumulate_points(points, voxel_size, voxels, class_hist)
            del points  # release tile before next query (bounded memory)

            total_points += n
            tiles += 1
            mem = rss_mb()
            peak = max(peak, mem)

            log.info(
                "tile %d | +%d pts (total %d) | voxels %d | RSS %.1f MB (peak %.1f)",
                tiles,
                n,
                total_points,
                len(voxels),
                mem,
                peak,
            )

            rows = voxels_to_rows(voxels, class_hist, voxel_size)

    log.info(
        "Done in %.1fs | tiles=%d points=%d voxels=%d peak_RSS=%.1f MB",
        time.perf_counter() - t0,
        tiles,
        total_points,
        len(rows),
        peak,
    )

    return rows

def main() -> None:
    rows = enrich()
    if not rows:
        log.warning("No voxels produced.")
        return

    log.info("Sample enriched rows:")
    for row in rows[:5]:
        log.info("  %s", row)

    log.info("Enrich OK — next: write these rows to GeoParquet.")


if __name__ == "__main__":
    main()
