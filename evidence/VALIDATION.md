# Executed validation

Verified on 2026-10-13 (Asia/Shanghai), Windows, Python 3.12.

- Automated suite: **26 passed, 0 failed, 0 skipped**.
- PostgreSQL server: **18.6**, actual temporary localhost instance.
- Pandas: **3.0.6**; Psycopg: **3.3.6**.
- CSV and JSON sample: 16 input records, 5 accepted, 9 rejected, 2 duplicates removed.
- Two actual loads: 4 customers and 5 orders remain in PostgreSQL.
- A database constraint violation during a batch rolled back both customer and
  order modifications; prior committed values remained unchanged.
- Invalid input coverage includes malformed CSV rows, nested mapped JSON values,
  duplicate JSON keys, non-standard JSON constants, monetary precision/range,
  large integer identifiers, missing/invalid fields, duplicate/conflicting data,
  and configuration/database field limits.
- Unmapped nested JSON metadata is ignored and tested.
- A deliberately failed connection containing a test password did not expose
  that password in stdout, stderr, logs, or summary artifacts.

`test_results.txt` and `test_results.xml` contain the executed test result.
`demo_results.json` contains queried PostgreSQL rows and both run summaries.
`demo_transcript.txt`, `clean.csv`, `rejected.csv`, `summary.json`, and
`pipeline.log` are captured from the final demonstration.

PostgreSQL binaries were obtained from the official EDB binary distribution:
https://www.enterprisedb.com/download-postgresql-binaries?lang=en
The binary archive was used only for local verification and is not distributed
in the delivery package. The temporary server was stopped after verification.
The command execution policy blocked removal of its local cache directory;
the cached runtime and disposable test database remain under `_research`, outside
the delivery package. There are no database credentials in the delivery package.

No remote client environment or private business dataset has been tested.
Business-specific acceptance and a paid contract are separate from this local
portfolio implementation.
