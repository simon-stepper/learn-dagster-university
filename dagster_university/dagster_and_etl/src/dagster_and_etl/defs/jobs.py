import dagster as dg

import dagster_and_etl.defs.assets as assets

import_partition_job = dg.define_asset_job(
    name="import_partition_job",
    selection=[
        assets.import_partition_file,
        assets.duckdb_partition_table,
    ],
)

import_dynamic_partition_job = dg.define_asset_job(
    name="import_dynamic_partition_job",
    selection=[
        assets.import_dynamic_partition_file,
        assets.duckdb_dynamic_partition_table,
    ],
)

asteroid_job = dg.define_asset_job(
    name="asteroid_job",
    selection=[
        assets.asteroids_partition,
        assets.asteroids_file,
        assets.duckdb_asteroids_table,
    ],
)