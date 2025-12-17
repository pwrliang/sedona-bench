#!/usr/bin/env python3
"""
Q9: Building Conflation - Duplicate/Overlap Detection via IoU
Tests GPU spatial join with self-join on building table
Detects duplicate or overlapping building footprints using Intersection over Union
"""
import sys
import time
import argparse
import sedonadb


def test_q9_with_external_tables(data_prefix=None, mode='gpu', repeat=5, target_partitions=None, building_limit=None):
    print(f"Testing Q9 Execution with {mode.upper()} using External Tables")
    if building_limit:
        print(f"Limit: building={building_limit}")
    else:
        print("No limits applied - using full dataset")
    print()

    # Connect and configure execution mode
    ctx = sedonadb.connect()
    if mode.lower() == 'gpu':
        ctx.sql("SET sedona.spatial_join.gpu.enable = true")
        print("GPU mode enabled")
    else:
        ctx.sql("SET sedona.spatial_join.gpu.enable = false")
        print("CPU mode enabled")

    # Configure target partitions if specified
    if target_partitions is not None:
        ctx.sql(f"SET datafusion.execution.target_partitions = {target_partitions}")
        print(f"Target partitions set to: {target_partitions}")

    print()

    # Create external table from Parquet files
    print("Creating external table from Parquet files...")
    ctx.sql(f"""
        CREATE EXTERNAL TABLE building_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/building/'
    """)

    print("External table created successfully")
    print()

    # Create views with parsed geometries
    print("Creating geometry views...")
    building_limit_clause = f"LIMIT {building_limit}" if building_limit else ""

    ctx.sql(f"""
        CREATE OR REPLACE VIEW building_geom AS
        SELECT b_buildingkey, ST_GeomFromWKB(b_boundary) as geom
        FROM building_table
        {building_limit_clause}
    """)

    print("View created successfully")
    print()

    # Show execution plan (should display GpuSpatialJoinExec when GPU is enabled)
    print(f"Execution plan for Q9 spatial join ({mode.upper()} mode):")
    plan = ctx.sql("""
        EXPLAIN
        WITH b1 AS (
            SELECT b_buildingkey AS id, geom
            FROM building_geom
        ), b2 AS (
            SELECT b_buildingkey AS id, geom
            FROM building_geom
        ), pairs AS (
            SELECT
                b1.id AS building_1,
                b2.id AS building_2,
                ST_Area(b1.geom) AS area1,
                ST_Area(b2.geom) AS area2,
                ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
            FROM b1
            JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom)
        ) SELECT
            building_1,
            building_2,
            area1,
            area2,
            overlap_area,
            CASE
                WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
                ELSE overlap_area / (area1 + area2 - overlap_area)
            END AS iou
        FROM pairs
        ORDER BY iou DESC, building_1 ASC, building_2 ASC
    """)
    plan.show()
    print()

    # Execute the Q9 query
    print("Running Q9 query ...")
    print("Query: Building conflation using IoU to detect overlapping buildings")
    print()

    start_time = time.time()
    for _ in range(repeat):
        result = ctx.sql("""
                         WITH b1 AS (SELECT b_buildingkey AS id, geom
                                     FROM building_geom),
                              b2 AS (SELECT b_buildingkey AS id, geom
                                     FROM building_geom),
                              pairs AS (SELECT b1.id                                      AS building_1,
                                               b2.id                                      AS building_2,
                                               ST_Area(b1.geom)                           AS area1,
                                               ST_Area(b2.geom)                           AS area2,
                                               ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
                                        FROM b1
                                                 JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom))
                         SELECT building_1,
                                building_2,
                                area1,
                                area2,
                                overlap_area,
                                CASE
                                    WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
                                    ELSE overlap_area / (area1 + area2 - overlap_area)
                                    END AS iou
                         FROM pairs
                         ORDER BY iou DESC, building_1 ASC, building_2 ASC
                         """)

        result.show(20)

    elapsed = (time.time() - start_time) / repeat

    print(f"Avg execution time ({mode.upper()} mode): {elapsed:.3f}s")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Q9 Spatial Join Performance Test')
    parser.add_argument('--data-prefix', '-d', type=str, default=None,
                        help='Prefix path of spatial-bench, e.g., "sf1" folder containing trip and zone (default: None)')
    parser.add_argument('mode', nargs='?', default='gpu', choices=['gpu', 'cpu'],
                        help='Execution mode: "gpu" or "cpu" (default: gpu)')
    parser.add_argument('--repeat', '-r', type=int, default=5,
                        help='Number of repeated runs (default: 5)')
    parser.add_argument('--partitions', '-p', type=int, default=None,
                        help='Number of target partitions for DataFusion (default: auto)')
    parser.add_argument('--building-limit', type=int, default=None,
                        help='LIMIT for building table (optional, no limit if not specified)')
    args = parser.parse_args()

    sys.exit(
        test_q9_with_external_tables(args.data_prefix, args.mode, args.repeat, args.partitions, args.building_limit))
