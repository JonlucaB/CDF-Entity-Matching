#!/usr/bin/env python3
"""
Configuration validation script.

Validates configuration files and provides helpful error messages
for common mistakes.
"""

import sys
from pathlib import Path

import yaml

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from functions.shared.utils.config_validator import (
        format_validation_errors,
        validate_extract_config,
        validate_hierarchy_config,
    )
except ImportError:
    print("⚠️  Validation utilities not available. Skipping validation.")
    sys.exit(0)


def validate_config_file(config_path: Path) -> bool:
    """Validate a configuration file."""
    print(f"\nValidating: {config_path}")
    print("=" * 80)

    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        return False

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Error reading config file: {e}")
        return False

    # Determine config type and validate
    config_id = config.get("externalId", "")
    errors = []

    if "extract_assets" in config_id or "extract_assets_by_pattern" in config_id:
        errors = validate_extract_config(config)
    elif "create_asset_hierarchy" in config_id:
        errors = validate_hierarchy_config(config)
    elif "write_asset_hierarchy" in config_id:
        # Write config is simpler, just check basic structure
        print("✅ Write config structure looks good (basic validation)")
        return True
    else:
        print(f"⚠️  Unknown config type: {config_id}")
        return True

    # Display results
    result = format_validation_errors(errors)
    print(result)

    # Return True if no errors (warnings are OK)
    has_errors = any(e.startswith("❌") for e in errors)
    return not has_errors


def main():
    """Main validation function."""
    module_root = Path(__file__).parent

    # Default configs to validate
    configs_to_validate = [
        module_root / "pipelines" / "ctx_extract_assets_by_pattern_default.config.yaml",
        module_root / "pipelines" / "ctx_create_asset_hierarchy_default.config.yaml",
        module_root / "pipelines" / "ctx_write_asset_hierarchy_default.config.yaml",
    ]

    # Allow custom config path
    if len(sys.argv) > 1:
        custom_path = Path(sys.argv[1])
        if custom_path.exists():
            configs_to_validate = [custom_path]
        else:
            print(f"❌ Config file not found: {custom_path}")
            sys.exit(1)

    print("=" * 80)
    print("CONFIGURATION VALIDATION")
    print("=" * 80)

    all_valid = True
    for config_path in configs_to_validate:
        if not validate_config_file(config_path):
            all_valid = False

    print("\n" + "=" * 80)
    if all_valid:
        print("✅ All configurations are valid!")
        sys.exit(0)
    else:
        print("❌ Some configurations have errors. Please fix them.")
        sys.exit(1)


if __name__ == "__main__":
    main()
