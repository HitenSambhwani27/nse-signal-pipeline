#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, "/tmp")
# reuse prove script with correct symbol
import runpy
sys.argv = ["prove", "2026-09-08", "NIFTY 50"]
runpy.run_path("/tmp/tmp_phase3_prove_compact.py", run_name="__main__")
