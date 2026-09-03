"""Stage 9 decision-quality tests — permanent evidence module.

Synthetic fills only. Keep this file as the Stage 9 entrypoint; the 2x2
implementation lives in test_account_decisions.py and is imported here so
pytest collects it under this name.
"""

from tests.test_account_decisions import (  # noqa: F401
    test_quality_2x2_and_anti_hindsight,
)
