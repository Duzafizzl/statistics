#!/usr/bin/env python3
"""
build_surface_dataset.py – Thin CLI wrapper (repo root). Runs the Appendix C surface pipeline.

Created: 2026-09-07
Last updated: 2026-09-07
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
runpy.run_module("appendix_c.build_surface_dataset", run_name="__main__")
