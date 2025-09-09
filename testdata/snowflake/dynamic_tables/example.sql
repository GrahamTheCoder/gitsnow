create or replace transient dynamic table my_schema.example_table (
    something integer not null
)
cluster by (trunc(something, -2))
target_lag = 'DOWNSTREAM' refresh_mode = incremental initialize = on_create warehouse = test_warehouse
as
(
    select 1 as something
);
