-- PostgreSQL. Run AFTER the MESHJOIN loader has populated data_warehouse.transactions.
-- Source data covers 2010-12-01 through 2011-12-09. Queries requesting "the year"
-- select MAX(dim_time.year) actually present in the loaded fact (normally 2011).
-- Q6 uses the six calendar months ending in the latest fact month (normally
-- July--December 2011); dates update automatically for partial loads.
-- Revenue always comes from the stored fact measure; no second fact is used.

-- Q1. Annual top five products for weekdays and weekends; monthly drill-down.
WITH report_year AS (
    SELECT MAX(d.year) AS yr
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
), annual AS (
    SELECT f.product_id, d.is_weekend, SUM(f.revenue) AS annual_revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    CROSS JOIN report_year y
    WHERE d.year = y.yr
    GROUP BY f.product_id, d.is_weekend
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY is_weekend ORDER BY annual_revenue DESC, product_id
    ) AS product_rank
    FROM annual
), monthly AS (
    SELECT f.product_id, d.is_weekend, d.year, d.month,
           SUM(f.revenue) AS monthly_revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    CROSS JOIN report_year y
    WHERE d.year = y.yr
    GROUP BY f.product_id, d.is_weekend, d.year, d.month
)
SELECT CASE WHEN m.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       r.product_rank, m.year, m.month, m.product_id, p.product_name,
       r.annual_revenue, m.monthly_revenue
FROM monthly m
JOIN ranked r USING (product_id, is_weekend)
JOIN data_warehouse.dim_product p USING (product_id)
WHERE r.product_rank <= 5
ORDER BY day_type, r.product_rank, m.year, m.month;

-- Q2. Customer demographics by gender, age group, and city category.
SELECT c.gender, c.age_group, c.city_category,
       COUNT(*) AS purchase_lines, SUM(f.revenue) AS total_purchases
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_customer c USING (customer_id)
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY c.gender, c.age_group, c.city_category;

-- Q3. Product category revenue by customer occupation.
SELECT p.product_category, c.occupation,
       SUM(f.quantity) AS units_sold, SUM(f.revenue) AS total_sales
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_product p USING (product_id)
JOIN data_warehouse.dim_customer c USING (customer_id)
GROUP BY p.product_category, c.occupation
ORDER BY p.product_category, total_sales DESC;

-- Q4. Quarterly purchases by gender and age for the latest loaded year.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
)
SELECT d.year, d.quarter, c.gender, c.age_group,
       SUM(f.revenue) AS purchase_amount
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_customer c USING (customer_id)
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
CROSS JOIN report_year y
WHERE d.year = y.yr
GROUP BY d.year, d.quarter, c.gender, c.age_group
ORDER BY d.year, d.quarter, c.gender, c.age_group;

-- Q5. Five highest-revenue occupations within each product category.
WITH totals AS (
    SELECT p.product_category, c.occupation, SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_product p USING (product_id)
    JOIN data_warehouse.dim_customer c USING (customer_id)
    GROUP BY p.product_category, c.occupation
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY product_category ORDER BY revenue DESC, occupation
    ) AS occupation_rank FROM totals
)
SELECT product_category, occupation_rank, occupation, revenue
FROM ranked WHERE occupation_rank <= 5
ORDER BY product_category, occupation_rank;

-- Q6. City category and marital status by month, latest six loaded months.
WITH bounds AS (
    SELECT DATE_TRUNC('month', MAX(d.date)) AS last_month
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
)
SELECT DATE_TRUNC('month', d.date)::date AS sales_month,
       c.city_category, c.marital_status, SUM(f.revenue) AS purchase_amount
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
JOIN data_warehouse.dim_customer c USING (customer_id)
CROSS JOIN bounds b
WHERE d.date >= b.last_month - INTERVAL '5 months'
  AND d.date <  b.last_month + INTERVAL '1 month'
GROUP BY sales_month, c.city_category, c.marital_status
ORDER BY sales_month, c.city_category, c.marital_status;

-- Q7. Average purchase/cart amount by length of city stay and gender.
-- One invoice (transaction_id) is one shopping cart; average CART totals.
WITH carts AS (
    SELECT f.transaction_id, f.customer_id, SUM(f.revenue) AS cart_amount
    FROM data_warehouse.transactions f
    GROUP BY f.transaction_id, f.customer_id
)
SELECT c.stay_in_current_city_years, c.gender,
       COUNT(*) AS carts, ROUND(AVG(x.cart_amount), 3) AS average_purchase_amount
FROM carts x
JOIN data_warehouse.dim_customer c USING (customer_id)
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;

-- Q8. Rank city categories A/B/C for revenue within each product category.
WITH totals AS (
    SELECT p.product_category, c.city_category, SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_product p USING (product_id)
    JOIN data_warehouse.dim_customer c USING (customer_id)
    GROUP BY p.product_category, c.city_category
)
SELECT product_category, city_category, revenue,
       RANK() OVER (PARTITION BY product_category ORDER BY revenue DESC) AS city_rank
FROM totals
ORDER BY product_category, city_rank, city_category;

-- Q9. Month-on-month revenue growth by product category, latest loaded year.
-- Months without sales count as zero; a zero prior month produces NULL growth.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
), months AS (
    SELECT GENERATE_SERIES(1, 12) AS month
), totals AS (
    SELECT p.product_category, d.month, SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    JOIN data_warehouse.dim_product p USING (product_id)
    CROSS JOIN report_year y
    WHERE d.year = y.yr
    GROUP BY p.product_category, d.month
), filled AS (
    SELECT y.yr AS year, p.product_category, m.month,
           COALESCE(t.revenue, 0::numeric) AS revenue
    FROM report_year y
    CROSS JOIN (SELECT DISTINCT product_category FROM data_warehouse.dim_product) p
    CROSS JOIN months m
    LEFT JOIN totals t ON t.product_category = p.product_category AND t.month = m.month
), growth AS (
    SELECT *, LAG(revenue) OVER (
        PARTITION BY product_category ORDER BY month
    ) AS previous_revenue FROM filled
)
SELECT year, month, product_category, revenue, previous_revenue,
       ROUND(100.0 * (revenue - previous_revenue) /
             NULLIF(previous_revenue, 0), 2) AS growth_pct
FROM growth ORDER BY product_category, month;

-- Q10. Compare weekday/weekend sales by age group in the loaded year.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
)
SELECT d.year, c.age_group,
       COALESCE(SUM(f.revenue) FILTER (WHERE NOT d.is_weekend), 0) AS weekday_sales,
       COALESCE(SUM(f.revenue) FILTER (WHERE d.is_weekend), 0) AS weekend_sales,
       SUM(f.revenue) AS total_sales
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
JOIN data_warehouse.dim_customer c USING (customer_id)
CROSS JOIN report_year y WHERE d.year = y.yr
GROUP BY d.year, c.age_group
ORDER BY d.year, c.age_group;

-- Q11. Top five products WITHIN each month and weekday/weekend group.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
), totals AS (
    SELECT d.year, d.month, d.is_weekend, f.product_id,
           SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    CROSS JOIN report_year y WHERE d.year = y.yr
    GROUP BY d.year, d.month, d.is_weekend, f.product_id
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY year, month, is_weekend ORDER BY revenue DESC, product_id
    ) AS product_rank FROM totals
)
SELECT r.year, r.month,
       CASE WHEN r.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       r.product_rank, r.product_id, p.product_name, r.revenue
FROM ranked r JOIN data_warehouse.dim_product p USING (product_id)
WHERE r.product_rank <= 5
ORDER BY r.year, r.month, day_type, r.product_rank;

-- Q12. Store quarter-on-quarter growth in the latest loaded year.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
), quarters AS (
    SELECT GENERATE_SERIES(1, 4) AS quarter
), totals AS (
    SELECT f.store_id, d.quarter, SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    CROSS JOIN report_year y WHERE d.year = y.yr
    GROUP BY f.store_id, d.quarter
), filled AS (
    SELECT y.yr AS year, s.store_id, s.store_name, q.quarter,
           COALESCE(t.revenue, 0::numeric) AS revenue
    FROM report_year y CROSS JOIN data_warehouse.dim_store s CROSS JOIN quarters q
    LEFT JOIN totals t ON t.store_id = s.store_id AND t.quarter = q.quarter
), growth AS (
    SELECT *, LAG(revenue) OVER (
        PARTITION BY store_id ORDER BY quarter
    ) AS previous_revenue FROM filled
)
SELECT year, store_name, quarter, revenue, previous_revenue,
       ROUND(100.0 * (revenue - previous_revenue) /
             NULLIF(previous_revenue, 0), 2) AS growth_pct
FROM growth ORDER BY store_name, quarter;

-- Q13. Supplier sales contribution within each store, broken down by product.
WITH detail AS (
    SELECT f.store_id, f.supplier_id, f.product_id, p.product_name,
           SUM(f.quantity) AS units_sold, SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_product p USING (product_id)
    GROUP BY f.store_id, f.supplier_id, f.product_id, p.product_name
)
SELECT s.store_name, su.supplier_name, x.product_id, x.product_name,
       x.units_sold, x.revenue,
       ROUND(100.0 * x.revenue /
             NULLIF(SUM(x.revenue) OVER (PARTITION BY x.store_id), 0), 2)
             AS pct_of_store_revenue
FROM detail x
JOIN data_warehouse.dim_store s USING (store_id)
JOIN data_warehouse.dim_supplier su USING (supplier_id)
ORDER BY s.store_name, su.supplier_name, x.product_name, x.product_id;

-- Q14. Product sales by year and season (Spring/Summer/Fall/Winter).
SELECT d.year, d.season, f.product_id, p.product_name,
       SUM(f.quantity) AS units_sold, SUM(f.revenue) AS total_sales
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
JOIN data_warehouse.dim_product p USING (product_id)
GROUP BY d.year, d.season, f.product_id, p.product_name
ORDER BY d.year,
         CASE d.season WHEN 'Spring' THEN 1 WHEN 'Summer' THEN 2
                       WHEN 'Fall' THEN 3 ELSE 4 END,
         f.product_id;

-- Q15. Month-to-month revenue change for each store/supplier pair.
WITH months AS (
    SELECT GENERATE_SERIES(
        DATE_TRUNC('month', MIN(d.date))::date,
        DATE_TRUNC('month', MAX(d.date))::date,
        INTERVAL '1 month'
    )::date AS month_start
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
), totals AS (
    SELECT f.store_id, f.supplier_id,
           DATE_TRUNC('month', d.date)::date AS month_start,
           SUM(f.revenue) AS revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    GROUP BY f.store_id, f.supplier_id, DATE_TRUNC('month', d.date)::date
), filled AS (
    SELECT s.store_id, s.store_name, su.supplier_id, su.supplier_name,
           m.month_start, COALESCE(t.revenue, 0::numeric) AS revenue
    FROM data_warehouse.dim_store s
    CROSS JOIN data_warehouse.dim_supplier su
    CROSS JOIN months m
    LEFT JOIN totals t ON t.store_id = s.store_id
      AND t.supplier_id = su.supplier_id AND t.month_start = m.month_start
), growth AS (
    SELECT *, LAG(revenue) OVER (
        PARTITION BY store_id, supplier_id ORDER BY month_start
    ) AS previous_revenue FROM filled
)
SELECT store_name, supplier_name, month_start, revenue, previous_revenue,
       ROUND(100.0 * (revenue - previous_revenue) /
             NULLIF(previous_revenue, 0), 2) AS volatility_pct
FROM growth ORDER BY store_name, supplier_name, month_start;

-- Q16. Five product pairs appearing together in the most DISTINCT carts.
WITH cart_products AS (
    SELECT DISTINCT transaction_id, product_id
    FROM data_warehouse.transactions
), pairs AS (
    SELECT a.product_id AS product_a, b.product_id AS product_b,
           COUNT(*) AS cart_count
    FROM cart_products a
    JOIN cart_products b ON b.transaction_id = a.transaction_id
     AND a.product_id < b.product_id
    GROUP BY a.product_id, b.product_id
)
SELECT x.product_a, pa.product_name AS name_a,
       x.product_b, pb.product_name AS name_b, x.cart_count
FROM pairs x
JOIN data_warehouse.dim_product pa ON pa.product_id = x.product_a
JOIN data_warehouse.dim_product pb ON pb.product_id = x.product_b
ORDER BY x.cart_count DESC, x.product_a, x.product_b
LIMIT 5;

-- Q17. Annual product/supplier/store subtotals and grand total via ROLLUP.
SELECT CASE WHEN GROUPING(d.year) = 1 THEN 'ALL YEARS'
            ELSE d.year::text END AS sales_year,
       CASE WHEN GROUPING(f.store_id) = 1 THEN 'ALL STORES'
            ELSE MIN(s.store_name) END AS store_name,
       CASE WHEN GROUPING(f.supplier_id) = 1 THEN 'ALL SUPPLIERS'
            ELSE MIN(su.supplier_name::text) END AS supplier_name,
       CASE WHEN GROUPING(f.product_id) = 1 THEN 'ALL PRODUCTS'
            ELSE MIN(p.product_name) END AS product_name,
       GROUPING(d.year, f.store_id, f.supplier_id, f.product_id) AS rollup_level,
       SUM(f.revenue) AS yearly_revenue
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
JOIN data_warehouse.dim_store s USING (store_id)
JOIN data_warehouse.dim_supplier su USING (supplier_id)
JOIN data_warehouse.dim_product p USING (product_id)
GROUP BY ROLLUP (d.year, f.store_id, f.supplier_id, f.product_id)
ORDER BY GROUPING(d.year, f.store_id, f.supplier_id, f.product_id),
         d.year NULLS LAST, f.store_id NULLS LAST,
         f.supplier_id NULLS LAST, f.product_id NULLS LAST;

-- Q18. Units and revenue by product: two half-years and full loaded year.
WITH report_year AS (
    SELECT MAX(d.year) AS yr FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
)
SELECT d.year, f.product_id, p.product_name,
       COALESCE(SUM(f.quantity) FILTER (WHERE d.half_year = 'First'), 0) AS first_half_qty,
       COALESCE(SUM(f.revenue) FILTER (WHERE d.half_year = 'First'), 0) AS first_half_revenue,
       COALESCE(SUM(f.quantity) FILTER (WHERE d.half_year = 'Second'), 0) AS second_half_qty,
       COALESCE(SUM(f.revenue) FILTER (WHERE d.half_year = 'Second'), 0) AS second_half_revenue,
       SUM(f.quantity) AS full_year_qty, SUM(f.revenue) AS full_year_revenue
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
JOIN data_warehouse.dim_product p USING (product_id)
CROSS JOIN report_year y WHERE d.year = y.yr
GROUP BY d.year, f.product_id, p.product_name
ORDER BY full_year_revenue DESC, f.product_id;

-- Q19. Product-day revenue greater than TWICE that product's average daily
-- revenue across the days on which that product recorded a sale.
-- Inspect returned rows (spike_date, daily_revenue, daily_average, spike_ratio)
-- to write a run-specific observation; no spike counts are assumed.
WITH daily AS (
    SELECT f.product_id, d.date AS spike_date, SUM(f.revenue) AS daily_revenue
    FROM data_warehouse.transactions f
    JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
    GROUP BY f.product_id, d.date
), scored AS (
    SELECT *, AVG(daily_revenue) OVER (PARTITION BY product_id) AS daily_average
    FROM daily
)
SELECT x.product_id, p.product_name, x.spike_date,
       x.daily_revenue, ROUND(x.daily_average, 3) AS daily_average,
       ROUND(x.daily_revenue / NULLIF(x.daily_average, 0), 2) AS spike_ratio
FROM scored x
JOIN data_warehouse.dim_product p USING (product_id)
WHERE x.daily_revenue > 2 * x.daily_average
ORDER BY spike_ratio DESC, x.daily_revenue DESC, x.product_id, x.spike_date;

-- Q20. Quarterly store sales view. Include YEAR to distinguish 2010/2011.
CREATE OR REPLACE VIEW data_warehouse.STORE_QUARTERLY_SALES AS
SELECT s.store_id, s.store_name, d.year, d.quarter,
       SUM(f.revenue) AS total_quarterly_sales
FROM data_warehouse.transactions f
JOIN data_warehouse.dim_store s USING (store_id)
JOIN data_warehouse.dim_time d ON d.time_key = f.time_key
GROUP BY s.store_id, s.store_name, d.year, d.quarter;

SELECT store_id, store_name, year, quarter, total_quarterly_sales
FROM data_warehouse.STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter;

-- Materialized snapshot required by the assignment's "Tasks Break-Up".
-- Refresh after subsequent fact loads; the named Q20 object remains a VIEW.
DROP MATERIALIZED VIEW IF EXISTS data_warehouse.store_quarterly_sales_mv;
CREATE MATERIALIZED VIEW data_warehouse.store_quarterly_sales_mv AS
SELECT * FROM data_warehouse.STORE_QUARTERLY_SALES
WITH DATA;
-- After later loads: REFRESH MATERIALIZED VIEW data_warehouse.store_quarterly_sales_mv;
