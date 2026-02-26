#!/usr/bin/env python3
"""
Create a sample asset hierarchy in the sp_enterprise_schema instance space.

Loads sample_asset_hierarchy.yaml from this directory and writes the assets
to CDF using the CogniteAsset view. Requires .env with CDF credentials
at the repository root.

Usage (from repo root):
  poetry run python modules/file_asset_hierarchy_extractor/scripts/create_sample_asset_hierarchy.py
  # or with dry run:
  poetry run python modules/file_asset_hierarchy_extractor/scripts/create_sample_asset_hierarchy.py --dry-run
"""

import argparse
import sys
from pathlib import Path

# Project root (key_extraction_aliasing)
repo_root = Path(__file__).resolve().parent.parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Load .env from repo root
_env_file = repo_root / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_env_file)
    except ImportError:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Create sample asset hierarchy in sp_enterprise_schema"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be written without writing to CDF",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    sample_yaml = script_dir / "sample_asset_hierarchy.yaml"
    if not sample_yaml.exists():
        print(f"Error: Sample YAML not found: {sample_yaml}")
        sys.exit(1)

    import yaml

    with open(sample_yaml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assets = data.get("items", [])
    if not assets:
        print("Error: No items in sample YAML")
        sys.exit(1)

    # Use write asset hierarchy handler dependencies and pipeline
    from modules.file_asset_hierarchy_extractor.functions.fn_dm_write_asset_hierarchy.dependencies import (
        create_client,
        create_logger_service,
        get_env_variables,
    )
    from modules.file_asset_hierarchy_extractor.functions.fn_dm_write_asset_hierarchy.pipeline import (
        write_asset_hierarchy,
    )

    try:
        env_config = get_env_variables()
        client = create_client(env_config)
    except Exception as e:
        print(f"Failed to create CDF client: {e}")
        print("Ensure .env has COGNITE_* or CDF_* credentials.")
        sys.exit(1)

    logger = create_logger_service("INFO")
    pipeline_data = {
        "assets": assets,
        "_local_mode": True,
        "batch_size": 100,
        "dry_run": args.dry_run,
        "view_space": "cdf_cdm",
        "view_external_id": "CogniteAsset",
        "view_version": "v1",
    }

    if args.dry_run:
        print("DRY RUN: No assets will be written to CDF.\n")

    try:
        write_asset_hierarchy(client=client, logger=logger, data=pipeline_data)
        stats = pipeline_data.get("stats", {})
        print(
            f"\nDone. Total: {stats.get('total', 0)}, "
            f"Created: {stats.get('created', 0)}, "
            f"Updated: {stats.get('updated', 0)}, "
            f"Failed: {stats.get('failed', 0)}"
        )
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
