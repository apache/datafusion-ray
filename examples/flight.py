import ray

import datafusion_ray
from datafusion_ray import DatafusionRayContext

## Prerequisites:
## $ brew install roapi
## $ roapi --table taxi=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
## $ maturin develop --features flight-sql-tables

ray.init()

ctx = datafusion_ray.extended_session_context({})

ray_ctx = DatafusionRayContext(ctx)

ray_ctx.sql("""
    CREATE EXTERNAL TABLE trip_data
    STORED AS FLIGHT_SQL
    LOCATION 'http://localhost:32010'
    OPTIONS (
        'flight.sql.query' 'SELECT * FROM taxi LIMIT 25'
    )
""")

df = ray_ctx.sql("SELECT tpep_pickup_datetime FROM trip_data LIMIT 10")
print(df.to_pandas())
