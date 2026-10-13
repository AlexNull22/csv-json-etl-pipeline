"""Run the sample twice and capture actual PostgreSQL evidence."""
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg

from etl import run_pipeline


def main():
    root = Path(__file__).resolve().parent
    url = os.environ["ETL_DATABASE_URL"]
    first, first_dir = run_pipeline(root / "config.json")
    second, second_dir = run_pipeline(root / "config.json")
    with psycopg.connect(url) as connection:
        version = connection.execute("SHOW server_version").fetchone()[0]
        customers = connection.execute("SELECT count(*) FROM small_etl.customers").fetchone()[0]
        orders = connection.execute("SELECT count(*) FROM small_etl.orders").fetchone()[0]
        rows = connection.execute(
            "SELECT order_id, customer_email, order_date::text, amount::text, currency, status "
            "FROM small_etl.orders ORDER BY order_id").fetchall()
    assert customers == 4 and orders == 5, "Run this demo against an empty demonstration database."
    assert first["loaded_rows"] == second["loaded_rows"] == 5
    evidence = {"verified_at": datetime.now().astimezone().isoformat(),
                "postgresql_version": version, "pandas_version": pd.__version__,
                "psycopg_version": psycopg.__version__, "first_run": first,
                "second_run": second, "database_customers": customers,
                "database_orders_after_two_runs": orders, "orders": rows}
    destination = root / "evidence"
    destination.mkdir(exist_ok=True)
    (destination / "demo_results.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for filename in ("clean.csv", "rejected.csv", "summary.json", "pipeline.log"):
        (destination / filename).write_bytes((second_dir / filename).read_bytes())
    print(json.dumps(evidence, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
