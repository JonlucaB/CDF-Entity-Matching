#!/usr/bin/env python3
"""
Script to run the create asset hierarchy function locally.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Import and run
from modules.file_asset_hierarchy_extractor.functions.fn_dm_create_asset_hierarchy.handler import (
    run_locally,
)

if __name__ == "__main__":
    print("=" * 80)
    try:
        result = run_locally()
        print("=" * 80)
        print(f"\n=== FINAL RESULT ===")
        print(f"Status: {result.get('status', 'unknown')}")
        if result.get("status") == "failure":
            print(f"Error: {result.get('message', 'Unknown error')}")
    except Exception as e:
        print("=" * 80)
        print(f"\n=== ERROR ===")
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
