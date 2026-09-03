# One-off diagnostics

Not production. Run from the repository root:

```powershell
$env:PYTHONPATH = "src"
python research/ops/_session_state.py
```

These scripts query local/VM SQLite for backfill and feature coverage. They must
not be wired into systemd.
