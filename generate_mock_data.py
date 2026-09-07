#!/usr/bin/env python3
"""
generate_mock_data.py – Thin CLI wrapper (repo root). Regenerates mock surface JSON.

Created: 2026-09-07
Last updated: 2026-09-07
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
runpy.run_module("appendix_c.generate_mock_data", run_name="__main__")
