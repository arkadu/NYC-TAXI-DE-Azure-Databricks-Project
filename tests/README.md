# Tests

These tests are intentionally local and lightweight. They verify repository contracts that should hold before a recruiter, interviewer, or CI runner sees the project:

- The Databricks bundle references files that actually exist.
- The Lakeview dashboard JSON is valid.
- Personal workspace/email values are not committed.
- The README has no merge-conflict markers or mojibake.

Run:

```powershell
python -m unittest discover -s tests
```

