"""
Exploratory Data Analysis for the dataset to check the CRS, columns, data types, and other information.

Usage (from src/):
  python eda.py
"""

from laspy import CopcReader

URL = "https://s3.amazonaws.com/hobu-lidar/sofi.copc.laz"

with CopcReader.open(URL) as reader:
    hdr = reader.header
    pf = hdr.point_format

    print("CRS:", hdr.parse_crs())
    print("Point count:", hdr.point_count)
    print("Bounds mins:", hdr.mins)
    print("Bounds maxs:", hdr.maxs)
    print("Point format id:", pf.id)
    print("Dimensions:", list(pf.dimension_names))

    for name in pf.dimension_names:
        dim = pf.dimension_by_name(name)
        print(f"  {name}: size={dim.num_bits} bits")  # LAS dimension info