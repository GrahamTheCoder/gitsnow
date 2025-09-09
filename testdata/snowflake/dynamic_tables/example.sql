create or replace transient dynamic table my_schema.example_table (
    something integer not null
)
target_lag = 'DOWNSTREAM' refresh_mode = incremental initialize = on_create warehouse = test_warehouse
cluster by (trunc(something, -2))
as
(
    select 1 as something
);
