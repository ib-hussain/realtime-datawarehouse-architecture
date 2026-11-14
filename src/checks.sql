-- Post-run measurements used to complete the 2-page report.
-- Run after loading. These queries do not modify warehouse data.
-- Cleaning counts (541909 / 397884 / 5192 / 392692) come from clean_data.py.

-- Actual master-data row and 500-tuple partition totals.
SELECT 'customer_master' AS relation, COUNT(*) AS rows,
       CEIL(COUNT(*) / 500.0)::integer AS partitions
FROM staging_area.customer_master
UNION ALL
SELECT 'product_master', COUNT(*), CEIL(COUNT(*) / 500.0)::integer
FROM staging_area.product_master;

-- Actual periodic group sizes (expected 48 groups; 2448 min, 2449 max
-- given the previously observed 117538 rows and the balanced splitter).
WITH group_sizes AS (
    SELECT release_hour, COUNT(*) AS rows
    FROM oltp_physical.periodic_sales_data
    GROUP BY release_hour
)
SELECT COUNT(*) AS release_groups, MIN(rows) AS smallest_group,
       MAX(rows) AS largest_group, SUM(rows) AS periodic_rows
FROM group_sizes;

-- Fact count, latest loaded year and loaded date range.
SELECT COUNT(*) AS warehouse_lines,
       COUNT(*) FILTER (WHERE f.sales_mode) AS online_lines,
       COUNT(*) FILTER (WHERE NOT f.sales_mode) AS periodic_lines,
       COUNT(*) FILTER (WHERE f.processing_mode = 'Shed') AS shed_then_loaded,
       MIN(d.date) AS first_loaded_date, MAX(d.date) AS last_loaded_date
FROM data_warehouse.transactions f
LEFT JOIN data_warehouse.dim_time d ON d.time_key = f.time_key;

-- Shed queue occupancy and recovery counts.
SELECT COUNT(*) AS ever_shed,
       COUNT(*) FILTER (WHERE processed) AS marked_processed,
       COUNT(*) FILTER (WHERE NOT processed) AS still_waiting,
       MIN(shed_at) AS first_shed_at, MAX(processed_at) AS last_recovery_at
FROM waiting_area.shed_table_physical;

-- Optional minute-level shedding trace (not a peak/non-peak classification).
SELECT DATE_TRUNC('minute', shed_at) AS shed_minute, COUNT(*) AS shed_rows
FROM waiting_area.shed_table_physical
GROUP BY DATE_TRUNC('minute', shed_at)
ORDER BY shed_minute;

-- Peak versus non-peak figures require mode-tagged runtime logs; the schema
-- records shed_at and processed_at but does not record the arrival mode.
