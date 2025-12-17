#!/usr/bin/env python3
"""
Q4: Zone Distribution of Top 1000 Trips by Tip Amount
Tests GPU spatial join with subquery and aggregation
Identifies which zones produced the most generous tippers
"""
import sys
import time
import argparse
import sedonadb


def test_q4_with_external_tables(data_prefix=None, mode='gpu', repeat=5, target_partitions=None, zone_limit=None,
                                 trip_limit=None,
                                 top_n=1000):
    print(f"Testing Q4 Execution with {mode.upper()} using External Tables")
    if zone_limit or trip_limit:
        print(f"Limits: zone={zone_limit if zone_limit else 'none'}, trip={trip_limit if trip_limit else 'none'}")
    else:
        print("No limits applied - using full dataset")
    print(f"Top N trips by tip: {top_n}")
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
        CREATE EXTERNAL TABLE zone_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/zone/'
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
    zone_limit_clause = f"LIMIT {zone_limit}" if zone_limit else ""
    trip_limit_clause = f"LIMIT {trip_limit}" if trip_limit else ""

    ctx.sql(f"""
        CREATE OR REPLACE VIEW zone_geom AS
        SELECT z_zonekey, z_name, ST_GeomFromWKB(z_boundary) as geom
        FROM zone_table
        {zone_limit_clause}
    """)

    ctx.sql(f"""
        CREATE OR REPLACE VIEW trip_geom AS
        SELECT t_tripkey, t_tip, ST_GeomFromWKB(t_pickuploc) as geom
        FROM trip_table
        {trip_limit_clause}
    """)

    print("Views created successfully")
    print()

    # Show execution plan (should display GpuSpatialJoinExec when GPU is enabled)
    print(f"Execution plan for Q4 spatial join ({mode.upper()} mode):")
    plan = ctx.sql(f"""
        EXPLAIN
        SELECT
            z.z_zonekey,
            z.z_name,
            COUNT(*) AS trip_count
        FROM
            zone_geom z
            JOIN (
                SELECT t.geom
                FROM trip_geom t
                ORDER BY t.t_tip DESC, t.t_tripkey ASC
                LIMIT {top_n}
            ) top_trips
            ON ST_Within(top_trips.geom, z.geom)
        GROUP BY z.z_zonekey, z.z_name
        ORDER BY trip_count DESC, z.z_zonekey ASC
    """)
    plan.show()
    print()

    # Execute the Q4 query
    print("Running Q4 query ...")
    print(f"Query: Zone distribution of top {top_n} trips by tip amount")
    print()

    start_time = time.time()
    for _ in range(repeat):
        result = ctx.sql(f"""
            SELECT
                z.z_zonekey,
                z.z_name,
                COUNT(*) AS trip_count
            FROM
                zone_geom z
                JOIN (
                    SELECT t.geom
                    FROM trip_geom t
                    ORDER BY t.t_tip DESC, t.t_tripkey ASC
                    LIMIT {top_n}
                ) top_trips
                ON ST_Within(top_trips.geom, z.geom)
            GROUP BY z.z_zonekey, z.z_name
            ORDER BY trip_count DESC, z.z_zonekey ASC
        """)
        result.show(20)

    elapsed = (time.time() - start_time) / repeat

    print(f"Avg execution time ({mode.upper()} mode): {elapsed:.3f}s")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Q4 Spatial Join Performance Test')
    parser.add_argument('--data-prefix', '-d', type=str, default=None,
                        help='Prefix path of spatial-bench, e.g., "sf1" folder containing trip and zone (default: None)')
    parser.add_argument('mode', nargs='?', default='gpu', choices=['gpu', 'cpu'],
                        help='Execution mode: "gpu" or "cpu" (default: gpu)')
    parser.add_argument('--repeat', '-r', type=int, default=5,
                        help='Number of repeated runs (default: 5)')
    parser.add_argument('--partitions', '-p', type=int, default=None,
                        help='Number of target partitions for DataFusion (default: auto)')
    parser.add_argument('--zone-limit', type=int, default=None,
                        help='LIMIT for zone table (optional, no limit if not specified)')
    parser.add_argument('--trip-limit', type=int, default=None,
                        help='LIMIT for trip table (optional, no limit if not specified)')
    parser.add_argument('--top-n', type=int, default=1000,
                        help='Number of top trips by tip to analyze (default: 1000)')
    args = parser.parse_args()

    sys.exit(
        test_q4_with_external_tables(args.data_prefix, args.mode, args.repeat, args.partitions, args.zone_limit,
                                     args.trip_limit,
                                     args.top_n))
