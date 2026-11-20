"""Curated read-only SQL and fixed relation choices exposed by the dashboard."""

CATALOG = {
    "Sales performance": """
        SELECT date_trunc('month', event_time)::date AS month,
               ROUND(SUM(revenue), 2) AS revenue,
               COUNT(*) AS lines
        FROM data_warehouse.transactions
        GROUP BY 1 ORDER BY 1
    """,
    "Sales mode comparison": """
        SELECT CASE WHEN sales_mode THEN 'Online' ELSE 'Physical' END AS channel,
               COUNT(*) AS lines, ROUND(SUM(revenue), 2) AS revenue
        FROM data_warehouse.transactions
        GROUP BY 1 ORDER BY revenue DESC
    """,
    "Top product categories": """
        SELECT p.product_category, COUNT(*) AS lines,
               ROUND(SUM(t.revenue), 2) AS revenue
        FROM data_warehouse.transactions t
        JOIN data_warehouse.dim_product p USING (product_id)
        GROUP BY 1 ORDER BY revenue DESC
    """,
    "Store leaderboard": """
        SELECT s.store_name, COUNT(*) AS lines,
               ROUND(SUM(t.revenue), 2) AS revenue
        FROM data_warehouse.transactions t
        JOIN data_warehouse.dim_store s USING (store_id)
        GROUP BY 1 ORDER BY revenue DESC
    """,
    "Top products": """
        SELECT p.product_name, COUNT(*) AS lines,
               ROUND(SUM(t.revenue), 2) AS revenue
        FROM data_warehouse.transactions t
        JOIN data_warehouse.dim_product p USING (product_id)
        GROUP BY 1 ORDER BY revenue DESC LIMIT 15
    """,
    "Top customers": """
        SELECT t.customer_id, COUNT(*) AS lines,
               ROUND(SUM(t.revenue), 2) AS revenue
        FROM data_warehouse.transactions t
        GROUP BY 1 ORDER BY revenue DESC LIMIT 20
    """,
    "Weekly throughput": """
        SELECT date_trunc('week', loaded_at)::date AS week, COUNT(*) AS loaded
        FROM data_warehouse.transactions
        GROUP BY 1 ORDER BY 1
    """,
    "Hourly sales distribution": """
        SELECT EXTRACT(HOUR FROM event_time)::int AS hour, COUNT(*) AS lines,
               ROUND(SUM(revenue), 2) AS revenue
        FROM data_warehouse.transactions
        GROUP BY 1 ORDER BY 1
    """,
    "Supplier performance": """
        SELECT s.supplier_name, COUNT(*) AS lines,
               ROUND(SUM(t.revenue), 2) AS revenue
        FROM data_warehouse.transactions t
        JOIN data_warehouse.dim_supplier s USING (supplier_id)
        GROUP BY 1 ORDER BY revenue DESC
    """,
    "Processing quality": """
        SELECT processing_mode,
               CASE WHEN sales_mode THEN 'Online' ELSE 'Periodic' END AS channel,
               COUNT(*) AS lines
        FROM data_warehouse.transactions
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "Cached online arrivals": """
        SELECT cache_id, transaction_id, source_row_id, customer_id,
               product_id, revenue, cached_at
        FROM data_warehouse.live_cache
        ORDER BY cache_id DESC LIMIT 100
    """,
    "Recent fact insertions": """
        SELECT transaction_line_id, transaction_id, source_type, source_row_id,
               store_id, revenue, loaded_at
        FROM data_warehouse.transactions
        ORDER BY transaction_line_id DESC LIMIT 100
    """,
    "Pending periodic shed": """
        SELECT COUNT(*) AS pending_rows
        FROM waiting_area.shed_table_physical
        WHERE processed = FALSE
    """,
}

RELATIONS = {
    "Warehouse transactions": "data_warehouse.transactions",
    "Online source": "oltp_online.online_sales_stream",
    "Periodic source": "oltp_physical.periodic_sales_data",
    "Online cache": "data_warehouse.live_cache",
    "Shed backlog": "waiting_area.shed_table_physical",
    "Customer dimension": "data_warehouse.dim_customer",
    "Product dimension": "data_warehouse.dim_product",
    "Customer master": "staging_area.customer_master",
    "Product master": "staging_area.product_master",
    "Preparation manifest": "staging_area.load_manifest",
}

CLI_COMMANDS = {
    "Check everything": "python cli_app.py doctor",
    "Initialize schemas (destructive)": "python cli_app.py init-db --yes",
    "Prepare CSVs": "python cli_app.py prepare",
    "Prepare and load PostgreSQL": "python cli_app.py prepare --load-db",
    "Start peak runtime": "python cli_app.py start --mode peak",
    "Switch to non-peak": "python cli_app.py control non-peak",
    "Show status": "python cli_app.py status",
    "Show logs": "python cli_app.py logs --lines 100",
    "Stop runtime": "python cli_app.py stop",
}
