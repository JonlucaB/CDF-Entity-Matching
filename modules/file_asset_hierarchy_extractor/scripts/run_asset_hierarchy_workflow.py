#!/usr/bin/env python3
"""
Workflow script that runs the complete asset hierarchy pipeline:
1. extract_assets_by_pattern - Extract assets from files using pattern matching
2. create_asset_hierarchy - Create hierarchical asset structure from extracted assets
3. write_asset_hierarchy - Write assets to CDF data modeling
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def run_workflow():
    """Run the complete asset hierarchy workflow."""
    print("=" * 80)
    print("Starting Asset Hierarchy Workflow")
    print("=" * 80)

    results = {}

    # Step 1: Extract assets by pattern
    print("\n" + "=" * 80)
    print("STEP 1: Extract Assets by Pattern")
    print("=" * 80)
    try:
        from modules.file_asset_hierarchy_extractor.functions.fn_dm_extract_assets_by_pattern.handler import (
            run_locally as run_extract,
        )

        extract_result = run_extract()
        if extract_result.get("status") != "succeeded":
            print(
                f"ERROR: Extract assets by pattern failed: {extract_result.get('message', 'Unknown error')}"
            )
            return {
                "status": "failure",
                "step": "extract_assets_by_pattern",
                "error": extract_result.get("message"),
            }

        results["extract_assets_by_pattern"] = extract_result
        print("✓ Step 1 completed successfully")

    except Exception as e:
        print(f"ERROR: Extract assets by pattern failed: {e}")
        import traceback

        traceback.print_exc()
        return {
            "status": "failure",
            "step": "extract_assets_by_pattern",
            "error": str(e),
        }

    # Step 2: Create asset hierarchy
    print("\n" + "=" * 80)
    print("STEP 2: Create Asset Hierarchy")
    print("=" * 80)
    try:
        from modules.file_asset_hierarchy_extractor.functions.fn_dm_create_asset_hierarchy.handler import (
            run_locally as run_create,
        )

        create_result = run_create()
        if create_result.get("status") != "succeeded":
            print(
                f"ERROR: Create asset hierarchy failed: {create_result.get('message', 'Unknown error')}"
            )
            return {
                "status": "failure",
                "step": "create_asset_hierarchy",
                "error": create_result.get("message"),
            }

        results["create_asset_hierarchy"] = create_result
        print("✓ Step 2 completed successfully")

    except Exception as e:
        print(f"ERROR: Create asset hierarchy failed: {e}")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "step": "create_asset_hierarchy", "error": str(e)}

    # Step 3: Write asset hierarchy
    print("\n" + "=" * 80)
    print("STEP 3: Write Asset Hierarchy")
    print("=" * 80)
    try:
        from modules.file_asset_hierarchy_extractor.functions.fn_dm_write_asset_hierarchy.handler import (
            run_locally as run_write,
        )

        write_result = run_write()
        if write_result.get("status") != "succeeded":
            print(
                f"ERROR: Write asset hierarchy failed: {write_result.get('message', 'Unknown error')}"
            )
            return {
                "status": "failure",
                "step": "write_asset_hierarchy",
                "error": write_result.get("message"),
            }

        results["write_asset_hierarchy"] = write_result
        print("✓ Step 3 completed successfully")

    except Exception as e:
        print(f"ERROR: Write asset hierarchy failed: {e}")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "step": "write_asset_hierarchy", "error": str(e)}

    # Summary
    print("\n" + "=" * 80)
    print("WORKFLOW COMPLETE")
    print("=" * 80)
    print("All steps completed successfully!")
    print("\nSummary:")
    print(
        f"  - Extract Assets: {results.get('extract_assets_by_pattern', {}).get('status', 'unknown')}"
    )
    print(
        f"  - Create Hierarchy: {results.get('create_asset_hierarchy', {}).get('status', 'unknown')}"
    )
    print(
        f"  - Write to CDF: {results.get('write_asset_hierarchy', {}).get('status', 'unknown')}"
    )

    return {"status": "succeeded", "results": results}


if __name__ == "__main__":
    try:
        result = run_workflow()
        print("\n" + "=" * 80)
        print(f"=== FINAL RESULT ===")
        print(f"Status: {result.get('status', 'unknown')}")
        if result.get("status") == "failure":
            print(f"Failed at step: {result.get('step', 'unknown')}")
            print(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 80)
        print(f"=== ERROR ===")
        print(f"Workflow failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
