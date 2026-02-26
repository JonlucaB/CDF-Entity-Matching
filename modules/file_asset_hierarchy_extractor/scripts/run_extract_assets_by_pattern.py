#!/usr/bin/env python3
"""Script to run extract_assets_by_pattern function locally."""

import sys
import traceback
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from modules.file_asset_hierarchy_extractor.functions.fn_dm_extract_assets_by_pattern.handler import (
        run_locally,
    )

    result = run_locally()
    print(f"\nFinal result: {result}")
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()
    sys.exit(1)
