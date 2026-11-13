"""Locations in the test suite checkout."""

from pathlib import Path

SOURCE_DIR = Path(__file__).resolve().parents[1]
ROOT = SOURCE_DIR.parent
ASSETS = ROOT / "assets"
