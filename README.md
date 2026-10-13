# CSV / JSON Order ETL

A small, reproducible Python/Pandas pipeline that reads CSV and JSON files,
standardizes order records, exports validation results, and loads valid records
into PostgreSQL. This is a self-initiated portfolio implementation based on a
public project description; it is not client work under an Upwork contract.
Development included AI assistance and independent review. The recorded
verification passed 26 tests, including two actual PostgreSQL integration tests.

## Quick start (no database required)

Python 3.11 or later is required. From this folder:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe etl.py --dry-run
```

On Linux/macOS, use `.venv/bin/python` in place of the Windows Python path.
Each execution creates a new folder under `output/`, containing `clean.csv`,
`rejected.csv`, `summary.json`, and `pipeline.log`.

## PostgreSQL execution

Use a PostgreSQL database dedicated to this example. Install PostgreSQL using
the [official Windows download page](https://www.postgresql.org/download/windows/)
or your normal database installation method. Create a database and a user with
permission to create a schema and tables, then set the connection string:

```powershell
$env:ETL_DATABASE_URL = 'postgresql://etl_user:YOUR_PASSWORD@127.0.0.1:5432/small_etl_demo'
.\.venv\Scripts\python.exe etl.py
```

For Linux/macOS: `export ETL_DATABASE_URL='postgresql://...'`.
URL-encode special characters in the username/password. Credentials are read
from the environment and are not included in configuration or logs. Connections
time out after 10 seconds. The schema in `schema.sql` is created automatically;
the script is also supplied for manual review/execution.

The `small_etl.customers` table has one row per email. `small_etl.orders` has one
row per order ID and a foreign key to the customer. Parameterized upserts run
within one transaction. Repeating a load updates matching keys instead of
creating duplicate rows; it does not delete orders absent from a later input.
Changes to an existing customer name or order are treated as updates from the
new input batch. A database error rolls back all database writes in that run.

## Configuration and sample schema

Edit `config.json`, not the source code, to change input paths, source column
names, permitted currencies/statuses, date formats, and output folder. Currency
codes must be 3 uppercase ASCII letters; status labels are lowercase and at most
32 characters. Paths
are resolved relative to the configuration file. Use `--config path/to/config.json`
to select another configuration. CSV is UTF-8 with an optional BOM; JSON is
an array of objects with flat mapped fields. Extra input columns are ignored,
including nested metadata. Nested JSON or boolean values in mapped fields,
duplicate JSON keys, malformed CSV widths, and missing required columns
stop the run before database loading. An empty file with a valid CSV header or
an empty JSON array is allowed.

The public brief does not prescribe business columns or cleaning policy. This
implementation uses an explicitly defined **fictional order dataset**:

| Field | Rule |
| --- | --- |
| order_id | Required text, 1–64 letters/digits/`_`/`-`; leading zeros preserved |
| customer_name | Required, whitespace collapsed, at most 200 characters; Unicode allowed |
| customer_email | Required, simple format validation; lowercased as an implementation assumption |
| order_date | Required, parsed using configured formats and exported as `YYYY-MM-DD` |
| amount | Required, nonnegative decimal, at most two decimal places, maximum 9999999999.99 |
| currency | Required, uppercased, one of the configured 3-letter codes |
| status | Lowercased; missing/blank values receive the configured default |

Money uses Decimal and PostgreSQL NUMERIC, without silent rounding. Missing
required values are rejected rather than fabricated. The email check tests
basic syntax, not mailbox existence. Dates are date-only; timezones are not
inferred. This batch implementation loads inputs into memory and is intended
for small files, not an unbounded streaming workload.

After normalization, identical records across all input files are deduplicated.
All differing valid versions of a repeated order ID are rejected. Different
customer names for the same email within a batch are also rejected. Invalid
records are excluded from the database and retained in `rejected.csv` with a
reason and source location. For CSV, `source_row` is the physical line where the
record ends; for JSON it is the 1-based array position. Summary counts satisfy
`input_rows = valid_rows + rejected_rows + duplicates_removed`.

Exit code 0 means the run completed with invalid records quarantined according
to this policy. Exit code 1 means an input/configuration/database failure. Always
review `rejected_rows`; a completed run may contain rejected data. A dry run never
connects to PostgreSQL. Output artifacts are local and may contain the input data.

## Demonstration and verification

The included 16 sample records intentionally contain missing fields, invalid
emails/dates/amounts, case differences, duplicate orders, and conflicting IDs.
Expected result: **5 valid orders, 9 rejected records, 2 duplicates removed**.
PostgreSQL should hold **4 customers and 5 orders** after either one or two runs.
The actual captured result is in `evidence/demo_results.json`.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe demo.py
```

`demo.py` requires `ETL_DATABASE_URL` pointing at an empty demonstration database.
It executes the sample twice and saves database query results. The tests include
real PostgreSQL checks for idempotence and complete transaction rollback. Without
a test database, those two tests are marked skipped. To run them, create a
separate, disposable localhost database named **small_etl_test**:

```powershell
$env:ETL_TEST_DATABASE_URL = 'postgresql://etl_user:YOUR_PASSWORD@127.0.0.1:5432/small_etl_test'
.\.venv\Scripts\python.exe -m pytest -q
```

The integration tests reset the `small_etl` schema only in that named localhost
test database. They never reset the demonstration database.
