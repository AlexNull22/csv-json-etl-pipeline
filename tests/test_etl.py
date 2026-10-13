import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import psycopg
import pytest

import etl

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config():
    return etl.load_config(ROOT / "config.json")


def sample_clean(config):
    raw = pd.concat([etl.read_input(x, ROOT) for x in config["inputs"]], ignore_index=True)
    return etl.transform(raw, config)


def record(**updates):
    row = dict(order_id="0001", customer_name="Alice Example", customer_email="alice@example.com",
               order_date="2026-10-01", amount="0.10", currency="USD", status="paid",
               source_file="test.csv", source_row=2)
    row.update(updates)
    return row


def test_sample_exact_expected_output(config):
    clean, rejected, duplicates = sample_clean(config)
    assert clean.order_id.tolist() == ["0001", "0002", "0003", "0010", "0011"]
    assert clean.amount.tolist() == ["49.90", "10.00", "20.50", "15.25", "0.00"]
    assert clean.customer_name.tolist() == ["Alice Example", "Bob Example", "Carol Example", "Bob Example", "李明"]
    assert len(rejected) == 9 and duplicates == 2
    assert set(rejected[rejected.order_id == "0008"].reason) == {"conflicting records for order_id"}
    assert 16 == len(clean) + len(rejected) + duplicates


@pytest.mark.parametrize("amount", ["-1", "NaN", "Infinity", "1.001", "1e3", "10000000000", "1,000"])
def test_invalid_amounts_rejected(amount, config):
    _, errors = etl.normalize(record(amount=amount), config)
    assert any("invalid amount" in x for x in errors)


def test_name_conflict_rejects_all_related_orders(config):
    raw = pd.DataFrame([record(), record(order_id="0002", customer_name="Different Name")])
    clean, rejected, duplicates = etl.transform(raw, config)
    assert clean.empty and len(rejected) == 2 and duplicates == 0


@pytest.mark.parametrize("extra", [",unexpected", ",unexpected,extra"])
def test_overwide_first_csv_row_fails(tmp_path, extra):
    path = tmp_path / "input.csv"
    path.write_text(",".join(etl.FIELDS) + "\n0001,Alice,alice@example.com,2026-10-01,5,USD,paid" + extra + "\n")
    with pytest.raises(etl.PipelineError, match="field count"):
        etl.read_input({"path": "input.csv"}, tmp_path)


def test_short_csv_row_fails(tmp_path):
    (tmp_path / "input.csv").write_text(",".join(etl.FIELDS) + "\n0001,Alice\n")
    with pytest.raises(etl.PipelineError, match="field count"):
        etl.read_input({"path": "input.csv"}, tmp_path)


def test_csv_literal_na_and_leading_zero_survive(tmp_path):
    (tmp_path / "input.csv").write_text(",".join(etl.FIELDS) + "\n0001,NA,na@example.com,2026-10-01,0,USD,\n")
    frame = etl.read_input({"path": "input.csv"}, tmp_path)
    assert frame.loc[0, "order_id"] == "0001" and frame.loc[0, "customer_name"] == "NA"


@pytest.mark.parametrize("contents", ['{"id":1}', '[{"order_id":"1","order_id":"2"}]',
                                          '[{"amount":NaN}]', '[{"amount":true}]', '[{"amount":{}}]'])
def test_bad_json_structure_fails(tmp_path, contents):
    (tmp_path / "input.json").write_text(contents)
    with pytest.raises(etl.PipelineError):
        etl.read_input({"path": "input.json"}, tmp_path)


def test_json_large_integer_is_not_rounded(tmp_path, config):
    row = {key: value for key, value in record().items() if key in etl.FIELDS}
    row["order_id"] = 9007199254740993
    (tmp_path / "input.json").write_text(json.dumps([row]), encoding="utf-8")
    clean, rejected, _ = etl.transform(etl.read_input({"path": "input.json"}, tmp_path), config)
    assert clean.loc[0, "order_id"] == "9007199254740993" and rejected.empty


def test_unmapped_nested_metadata_is_ignored(tmp_path, config):
    row = {key: value for key, value in record().items() if key in etl.FIELDS}
    row["metadata"] = {"notes": ["not used"], "enabled": True}
    (tmp_path / "input.json").write_text(json.dumps([row]), encoding="utf-8")
    clean, rejected, _ = etl.transform(etl.read_input({"path": "input.json"}, tmp_path), config)
    assert len(clean) == 1 and rejected.empty


@pytest.mark.parametrize("change", [{"allowed_currencies": ["US"]},
                                    {"allowed_statuses": ["pending", "A" * 33]}])
def test_config_must_match_database_field_bounds(tmp_path, change):
    cfg = {"inputs": [{"path": "unused.csv"}], **change}
    (tmp_path / "config.json").write_text(json.dumps(cfg))
    with pytest.raises(etl.PipelineError):
        etl.load_config(tmp_path / "config.json")


def test_dry_run_outputs_match_and_do_not_connect(tmp_path, config, monkeypatch):
    config["inputs"] = [{"path": str(ROOT / "samples/orders.csv")}]
    config.pop("base_dir")
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    def forbidden(*args, **kwargs):
        raise AssertionError("dry-run must not connect")
    monkeypatch.setattr(etl, "load_database", forbidden)
    summary, output = etl.run_pipeline(path, dry_run=True)
    assert summary["valid_rows"] == 4 and summary["database_status"] == "not_attempted"
    assert (output / "clean.csv").is_file() and (output / "rejected.csv").is_file()
    assert json.loads((output / "summary.json").read_text()) == summary


def test_connection_failure_does_not_log_secret(tmp_path):
    cfg = {"inputs": [{"path": str(ROOT / "samples/orders.csv")}]}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    secret = "test-secret-never-log"
    env = dict(os.environ, ETL_DATABASE_URL=f"postgresql://test:{secret}@127.0.0.1:1/missing")
    result = subprocess.run([sys.executable, str(ROOT / "etl.py"), "--config", str(path)],
                            env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 1 and secret not in result.stdout + result.stderr
    artifacts = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "output").rglob("*") if p.is_file())
    assert secret not in artifacts
    summaries = list((tmp_path / "output").rglob("summary.json"))
    assert json.loads(summaries[0].read_text())["database_status"] == "failed"


@pytest.fixture
def database_url():
    url = os.environ.get("ETL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ETL_TEST_DATABASE_URL for real PostgreSQL integration tests")
    info = psycopg.conninfo.conninfo_to_dict(url)
    if info.get("dbname") != "small_etl_test" or info.get("host") not in {"127.0.0.1", "localhost"}:
        pytest.fail("Integration tests only reset a localhost database named small_etl_test")
    with psycopg.connect(url) as connection:
        connection.execute("DROP SCHEMA IF EXISTS small_etl CASCADE")
    return url


def test_postgres_repeat_is_idempotent(database_url, config):
    clean, _, _ = sample_clean(config)
    etl.load_database(clean, database_url)
    etl.load_database(clean, database_url)
    with psycopg.connect(database_url) as connection:
        assert connection.execute("SELECT count(*) FROM small_etl.orders").fetchone()[0] == 5
        assert connection.execute("SELECT count(*) FROM small_etl.customers").fetchone()[0] == 4
        assert str(connection.execute("SELECT amount FROM small_etl.orders WHERE order_id='0001'").fetchone()[0]) == "49.90"
        assert connection.execute("SELECT name FROM small_etl.customers WHERE email='li@example.com'").fetchone()[0] == "李明"


def test_postgres_failure_rolls_back_whole_batch(database_url, config):
    clean, _, _ = sample_clean(config)
    etl.load_database(clean.iloc[:1], database_url)
    with psycopg.connect(database_url) as connection:
        connection.execute("ALTER TABLE small_etl.orders ADD CONSTRAINT reject_zero CHECK (amount > 0)")
    changed = clean.copy()
    changed.loc[0, "amount"] = "99.00"
    changed.loc[0, "customer_name"] = "Changed Name"
    with pytest.raises(psycopg.errors.CheckViolation):
        etl.load_database(changed, database_url)
    with psycopg.connect(database_url) as connection:
        assert connection.execute("SELECT count(*) FROM small_etl.orders").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM small_etl.customers").fetchone()[0] == 1
        assert str(connection.execute("SELECT amount FROM small_etl.orders").fetchone()[0]) == "49.90"
        assert connection.execute("SELECT name FROM small_etl.customers").fetchone()[0] == "Alice Example"
