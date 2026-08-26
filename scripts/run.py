#!/usr/bin/env python3
"""CLI entry without installation: python scripts/run.py --demo"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dgx_spark_memory_dashboard.server import main

if __name__ == "__main__":
    raise SystemExit(main())
