"""CSV/JSON -> validated order records -> PostgreSQL. Python 3.11+."""

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

import pandas as pd
import psycopg

FIELDS = ["order_id", "customer_name", "customer_email", "order_date",
          "amount", "currency", "status"]
REQUIRED = set(FIELDS) - {"status"}
PROVENANCE = ["source_file", "source_row"]
LOGGER = logging.getLogger("small_etl")


class PipelineError(ValueError):
    """An input or configuration error which should stop the entire run."""


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError(f"Duplicate JSON field: {key}.")
        result[key] = value
    return result


def reject_json_constant(value):
    raise PipelineError(f"Non-standard JSON numeric constant: {value}.")


def load_config(path):
    path = Path(path).resolve()
    with path.open(encoding="utf-8-sig") as stream:
        config = json.load(stream, object_pairs_hook=unique_json_object)
    if not isinstance(config, dict) or not isinstance(config.get("inputs"), list):
        raise PipelineError("Configuration must contain an inputs array.")
    if not config["inputs"]:
        raise PipelineError("Configure at least one input file.")
    config["base_dir"] = path.parent
    config.setdefault("output_dir", "output")
    config.setdefault("allowed_currencies", ["USD", "EUR", "GBP"])
    config.setdefault("allowed_statuses", ["pending", "paid", "cancelled"])
    config.setdefault("default_status", "pending")
    config.setdefault("date_formats", ["%Y-%m-%d"])
    for key in ("allowed_currencies", "allowed_statuses", "date_formats"):
        if not isinstance(config[key], list) or not config[key] or not all(
                isinstance(item, str) and item for item in config[key]):
            raise PipelineError(f"{key} must be a non-empty array of strings.")
    if not all(re.fullmatch(r"[A-Z]{3}", item) for item in config["allowed_currencies"]):
        raise PipelineError("Currencies must be 3 uppercase ASCII letters.")
    if not all(item == item.lower() and len(item) <= 32
               for item in config["allowed_statuses"]):
        raise PipelineError("Statuses must be lowercase and at most 32 characters.")
    if config["default_status"] not in config["allowed_statuses"]:
        raise PipelineError("Default status must be an allowed status.")
    return config


def read_input(spec, base_dir):
    if not isinstance(spec, dict) or not isinstance(spec.get("path"), str):
        raise PipelineError("Each input must have a path.")
    path = (base_dir / spec["path"]).resolve()
    mapping = spec.get("column_map", {})
    if not isinstance(mapping, dict) or not all(
            isinstance(k, str) and v in FIELDS for k, v in mapping.items()):
        raise PipelineError("Column mappings must map source names to standard fields.")
    if path.suffix.lower() == ".csv":
        # Validate every width: Pandas can infer an index from an over-wide first row.
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, [])
            data, row_numbers = [], []
            for values in reader:
                if not values:
                    continue
                if len(values) != len(header):
                    raise PipelineError(f"{path.name}: incorrect field count at line {reader.line_num}.")
                data.append(values)
                row_numbers.append(reader.line_num)
        if not header or len(header) != len(set(header)):
            raise PipelineError(f"{path.name}: empty or duplicate CSV header.")
        frame = pd.DataFrame(data, columns=header, dtype=object)
    elif path.suffix.lower() == ".json":
        with path.open(encoding="utf-8-sig") as stream:
            records = json.load(stream, parse_float=Decimal, parse_int=str,
                                parse_constant=reject_json_constant,
                                object_pairs_hook=unique_json_object)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise PipelineError(f"{path.name}: JSON must be an array of objects.")
        if any(isinstance(value, (dict, list, bool))
               for row in records for key, value in row.items()
               if mapping.get(key, key) in FIELDS):
            raise PipelineError(f"{path.name}: JSON fields must be scalar text/numbers/null.")
        frame = pd.DataFrame(records, dtype=object) if records else pd.DataFrame(columns=FIELDS)
        row_numbers = list(range(1, len(frame) + 1))
    else:
        raise PipelineError(f"{path.name}: only CSV and JSON are supported.")
    frame = frame.rename(columns=mapping)
    if frame.columns.duplicated().any():
        raise PipelineError(f"{path.name}: mapping produces duplicate field names.")
    if REQUIRED - set(frame.columns):
        missing = ", ".join(sorted(REQUIRED - set(frame.columns)))
        raise PipelineError(f"{path.name}: missing required columns: {missing}.")
    frame = frame.reindex(columns=FIELDS).fillna("")
    frame["source_file"] = spec["path"]
    # CSV uses the physical line where the record ends; JSON uses object position.
    frame["source_row"] = row_numbers
    return frame


def normalize(row, config):
    result = {key: " ".join(str(row[key]).split()) for key in FIELDS}
    errors = []
    for key in REQUIRED:
        if not result[key]:
            errors.append(f"missing {key}")
    if result["order_id"] and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", result["order_id"]):
        errors.append("invalid order_id (use 1-64 letters/digits/underscore/hyphen)")
    if len(result["customer_name"]) > 200:
        errors.append("customer_name exceeds 200 characters")
    result["customer_email"] = result["customer_email"].lower()
    if result["customer_email"] and (len(result["customer_email"]) > 254 or not re.fullmatch(
            r"[^\s@]+@[^\s@]+\.[^\s@]+", result["customer_email"])):
        errors.append("invalid customer_email")
    if result["order_date"]:
        for fmt in config["date_formats"]:
            try:
                result["order_date"] = datetime.strptime(result["order_date"], fmt).date().isoformat()
                break
            except ValueError:
                continue
        else:
            errors.append("invalid order_date")
    if result["amount"]:
        try:
            if not re.fullmatch(r"\d+(?:\.\d{1,2})?", result["amount"]):
                raise InvalidOperation
            amount = Decimal(result["amount"])
            if amount > Decimal("9999999999.99"):
                raise InvalidOperation
            result["amount"] = format(amount, ".2f")
        except InvalidOperation:
            errors.append("invalid amount (0..9999999999.99, at most 2 decimals)")
    result["currency"] = result["currency"].upper()
    if result["currency"] and result["currency"] not in config["allowed_currencies"]:
        errors.append("unsupported currency")
    result["status"] = result["status"].lower() or config["default_status"]
    if result["status"] not in config["allowed_statuses"]:
        errors.append("unsupported status")
    result.update({key: row[key] for key in PROVENANCE})
    return result, errors


def transform(raw, config):
    accepted, rejected = [], []
    for row in raw.to_dict("records"):
        normalized, errors = normalize(row, config)
        if errors:
            rejected.append({**row, "reason": "; ".join(errors)})
        else:
            accepted.append(normalized)
    valid = pd.DataFrame(accepted, columns=FIELDS + PROVENANCE)
    duplicates = int(valid.duplicated(subset=FIELDS).sum())
    valid = valid.drop_duplicates(subset=FIELDS, keep="first")
    # Reject every version of conflicting IDs, rather than arbitrarily picking one.
    conflict_ids = set(valid.loc[valid.duplicated("order_id", keep=False), "order_id"])
    for row in valid[valid["order_id"].isin(conflict_ids)].to_dict("records"):
        rejected.append({**row, "reason": "conflicting records for order_id"})
    valid = valid[~valid["order_id"].isin(conflict_ids)]
    name_counts = valid.groupby("customer_email")["customer_name"].nunique()
    conflict_emails = set(name_counts[name_counts > 1].index)
    for row in valid[valid["customer_email"].isin(conflict_emails)].to_dict("records"):
        rejected.append({**row, "reason": "conflicting customer names for email"})
    valid = valid[~valid["customer_email"].isin(conflict_emails)]
    valid = valid.sort_values("order_id").reset_index(drop=True)
    invalid = pd.DataFrame(rejected, columns=FIELDS + PROVENANCE + ["reason"])
    return valid, invalid, duplicates


def load_database(clean, database_url):
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    # Schema creation and all upserts share one transaction; any failure rolls back.
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        connection.execute(schema)
        customers = clean[["customer_email", "customer_name"]].drop_duplicates()
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO small_etl.customers (email, name) VALUES (%s, %s) "
                "ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name",
                list(customers.itertuples(index=False, name=None)),
            )
            cursor.executemany(
                "INSERT INTO small_etl.orders "
                "(order_id, customer_email, order_date, amount, currency, status) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (order_id) DO UPDATE SET "
                "customer_email=EXCLUDED.customer_email, order_date=EXCLUDED.order_date, "
                "amount=EXCLUDED.amount, currency=EXCLUDED.currency, status=EXCLUDED.status",
                [(r.order_id, r.customer_email, r.order_date, Decimal(r.amount), r.currency, r.status)
                 for r in clean.itertuples(index=False)],
            )
    return len(clean)


def run_pipeline(config_path, dry_run=False):
    config = load_config(config_path)
    database_url = os.environ.get("ETL_DATABASE_URL")
    if not dry_run and not database_url:
        raise PipelineError("Set ETL_DATABASE_URL, or use --dry-run.")
    run_dir = config["base_dir"] / config["output_dir"] / (
        datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:8])
    run_dir.mkdir(parents=True, exist_ok=False)
    handler = logging.FileHandler(run_dir / "pipeline.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    summary = {"run_id": run_dir.name, "mode": "dry-run" if dry_run else "database",
               "status": "failed", "database_status": "not_attempted",
               "input_rows": 0, "valid_rows": 0, "rejected_rows": 0,
               "duplicates_removed": 0, "loaded_rows": 0}
    try:
        frames = []
        for spec in config["inputs"]:
            frame = read_input(spec, config["base_dir"])
            frames.append(frame)
            LOGGER.info("Read input %s: %d rows", spec["path"], len(frame))
        raw = pd.concat(frames, ignore_index=True)
        clean, rejected, duplicates = transform(raw, config)
        clean.to_csv(run_dir / "clean.csv", index=False, encoding="utf-8")
        rejected.to_csv(run_dir / "rejected.csv", index=False, encoding="utf-8")
        summary.update(input_rows=len(raw), valid_rows=len(clean),
                       rejected_rows=len(rejected), duplicates_removed=duplicates)
        LOGGER.info("Validation: %d valid, %d rejected, %d duplicates removed",
                    len(clean), len(rejected), duplicates)
        if not dry_run:
            summary["database_status"] = "failed"
            summary["loaded_rows"] = load_database(clean, database_url)
            summary["database_status"] = "committed"
            LOGGER.info("PostgreSQL transaction committed: %d orders", len(clean))
        summary["status"] = "completed"
        return summary, run_dir
    except Exception as error:
        # Connection errors can contain credentials. Keep details out of artifacts.
        summary["error_type"] = type(error).__name__
        LOGGER.error("Run failed (%s). Database transaction was not committed.", type(error).__name__)
        raise
    finally:
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        LOGGER.removeHandler(handler)
        handler.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).with_name("config.json")))
    parser.add_argument("--dry-run", action="store_true", help="Validate/export without a database.")
    args = parser.parse_args()
    try:
        summary, output = run_pipeline(args.config, args.dry_run)
    except (PipelineError, FileNotFoundError, json.JSONDecodeError, csv.Error, pd.errors.ParserError,
            pd.errors.EmptyDataError) as error:
        print(f"Input/configuration error: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"Pipeline failed ({type(error).__name__}). Check input, output permissions "
              "and database connectivity. Credentials are not logged.", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2))
    print(f"Outputs: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
