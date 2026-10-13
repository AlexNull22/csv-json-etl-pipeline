"""Build an explicit, reviewed delivery archive; omit runtime and credentials."""
import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(__file__).resolve().parent
filenames = ["etl.py", "demo.py", "schema.sql", "config.json", "README.md",
             "PORTFOLIO.md", "requirements.txt", "requirements-dev.txt",
             "samples/orders.csv", "samples/orders.json", "tests/test_etl.py",
             "evidence/VALIDATION.md", "evidence/test_results.txt", "evidence/test_results.xml",
             "evidence/demo_results.json", "evidence/demo_transcript.txt", "evidence/clean.csv",
             "evidence/rejected.csv", "evidence/summary.json", "evidence/pipeline.log"]
archive_path = root / "ETL_Delivery.zip"
manifest = {}
with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
    for filename in filenames:
        data = (root / filename).read_bytes()
        manifest[filename] = hashlib.sha256(data).hexdigest()
        archive.writestr(filename, data)
    archive.writestr("MANIFEST.json", json.dumps(manifest, indent=2) + "\n")
with ZipFile(archive_path) as archive:
    assert archive.testzip() is None
    assert set(archive.namelist()) == set(filenames) | {"MANIFEST.json"}
    for filename, expected_hash in manifest.items():
        assert hashlib.sha256(archive.read(filename)).hexdigest() == expected_hash
        assert archive.read(filename) == (root / filename).read_bytes()
(root / "evidence/package_verification.json").write_text(json.dumps({
    "archive": archive_path.name, "file_count": len(filenames) + 1,
    "all_files_match_source": True, "zip_integrity_passed": True,
    "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    "excluded": [".venv", "output", "review", "PostgreSQL runtime", "credentials"]
}, indent=2) + "\n", encoding="utf-8")
print(f"Verified {archive_path.name}: {len(filenames) + 1} files, {archive_path.stat().st_size} bytes")
