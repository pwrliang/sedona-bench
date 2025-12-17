#!/usr/bin/env python3
"""
Q8: Spatial Join Performance Test - CPU vs GPU Comparison
Tests GPU spatial join using ST_Within for buildings containing trip pickups
Note: Using ST_Within instead of ST_DWithin for GPU compatibility
"""
import sys
import time
import argparse
import sedonadb

def test_q8_with_external_tables(data_prefix=None, mode='gpu', repeat=5, target_partitions=None, building_limit=None, trip_limit=None):
    print(f"Testing Q8 Execution with {mode.upper()} using External Tables")
    if building_limit or trip_limit:
        print(f"Limits: building={building_limit if building_limit else 'none'}, trip={trip_limit if trip_limit else 'none'}")
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

    # Create external tables from Parquet files
    print("Creating external tables from Parquet files...")
    ctx.sql(f"""
        CREATE EXTERNAL TABLE building_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/building/'
    """)

    ctx.sql(f"""
        CREATE EXTERNAL TABLE trip_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/trip/'
    """)

    print("External tables created successfully")
    print()

    # Create views with parsed geometries
    print("Creating geometry views...")
    building_limit_clause = f"LIMIT {building_limit}" if building_limit else ""
    trip_limit_clause = f"LIMIT {trip_limit}" if trip_limit else ""

    ctx.sql(f"""
        CREATE OR REPLACE VIEW building_geom AS
        SELECT b_buildingkey, b_name, ST_GeomFromWKB(b_boundary) as geom
        FROM building_table
        {building_limit_clause}
    """)

    ctx.sql(f"""
        CREATE OR REPLACE VIEW trip_geom AS
        SELECT ST_GeomFromWKB(t_pickuploc) as geom
        FROM trip_table
        {trip_limit_clause}
    """)

    print("Views created successfully")
    print()

    # Show execution plan (should display GpuSpatialJoinExec when GPU is enabled, SpatialJoinExec for CPU)
    print(f"Execution plan for spatial join ({mode.upper()} mode):")
    plan = ctx.sql("""
        EXPLAIN
        SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
        FROM trip_geom t
        JOIN building_geom b
        ON ST_Within(t.geom, b.geom)
        GROUP BY b.b_buildingkey, b.b_name
        ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
    """)
    plan.show()
    print()

    # Execute the spatial join query
    print("Running Q8 query ...")
    print("Query: Count pickups within each building using ST_Within")
    print()

    start_time = time.time()
    for _ in range(repeat):
        result = ctx.sql("""
            SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
            FROM trip_geom t
            JOIN building_geom b
            ON ST_Within(t.geom, b.geom)
            GROUP BY b.b_buildingkey, b.b_name
            ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
        """)

        result.show(20)

    elapsed = (time.time() - start_time) / repeat

    print(f"Avg execution time ({mode.upper()} mode): {elapsed:.3f}s")

    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Q8 Spatial Join Performance Test')
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
    parser.add_argument('--trip-limit', type=int, default=None,
                        help='LIMIT for trip table (optional, no limit if not specified)')
    args = parser.parse_args()

    sys.exit(test_q8_with_external_tables(args.data_prefix, args.mode, args.partitions, args.building_limit, args.trip_limit))
