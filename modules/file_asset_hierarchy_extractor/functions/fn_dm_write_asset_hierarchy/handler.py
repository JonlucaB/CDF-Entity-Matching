"""
CDF Handler for Write Asset Hierarchy

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly.
"""

from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

# Try to import CDF config loader
try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    load_config_parameters = None
    Config = None


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for write asset hierarchy.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline (optional)
            - hierarchy_file: Path to asset hierarchy YAML file (optional if assets provided)
            - assets: List of asset dictionaries (optional if hierarchy_file provided)
            - batch_size: Number of assets to process per batch (default: 100)
            - dry_run: Show what would be written without actually writing (default: False)
            - view_space: View space for CogniteAsset (default: cdf_cdm)
            - view_external_id: View external ID for CogniteAsset (default: CogniteAsset)
            - view_version: View version for CogniteAsset (default: v1)
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required)

    Returns:
        Dictionary with status and result information
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")

        # Use logger from dependencies
        from .dependencies import create_logger_service

        logger = create_logger_service(loglevel)

        logger.info(f"Starting write asset hierarchy with loglevel = {loglevel}")

        # Load configuration from CDF extraction pipeline
        cdf_config = None
        if CDF_CONFIG_AVAILABLE and client and "ExtractionPipelineExtId" in data:
            pipeline_ext_id = data["ExtractionPipelineExtId"]
            logger.info(f"Loading config from extraction pipeline: {pipeline_ext_id}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

            # Extract parameters from CDF config and merge into data
            config_data = cdf_config.data
            config_params = cdf_config.parameters

            # Read from parameters section
            if "raw_db" not in data:
                data["raw_db"] = config_params.raw_db
            if "raw_table_assets" not in data:
                data["raw_table_assets"] = config_params.raw_table_assets

            # Read from data section (for backward compatibility and local runs)
            if "hierarchy_file" not in data and "hierarchy_file" in config_data:
                data["hierarchy_file"] = config_data["hierarchy_file"]
            if "batch_size" not in data and "batch_size" in config_data:
                data["batch_size"] = config_data.get("batch_size", 100)
            if "dry_run" not in data and "dry_run" in config_data:
                data["dry_run"] = config_data.get("dry_run", False)
            if "view_space" not in data and "view_space" in config_data:
                data["view_space"] = config_data.get("view_space", "cdf_cdm")
            if "view_external_id" not in data and "view_external_id" in config_data:
                data["view_external_id"] = config_data.get(
                    "view_external_id", "CogniteAsset"
                )
            if "view_version" not in data and "view_version" in config_data:
                data["view_version"] = config_data.get("view_version", "v1")
            if "min_confidence" not in data and "min_confidence" in config_data:
                data["min_confidence"] = config_data["min_confidence"]
            if "logLevel" not in data:
                data["logLevel"] = "DEBUG" if cdf_config.parameters.debug else "INFO"

            # Store CDF config for use in pipeline
            data["_cdf_config"] = cdf_config

        # Check if client is provided
        if client is None:
            if not CDF_AVAILABLE:
                raise ImportError("CogniteClient not available. Install cognite-sdk.")
            # Try to create client from dependencies
            from .dependencies import create_client, get_env_variables

            env_config = get_env_variables()
            client = create_client(env_config)

        # Call pipeline function
        from .pipeline import write_asset_hierarchy

        write_asset_hierarchy(
            client=client,
            logger=logger,
            data=data,
        )

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Write asset hierarchy pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file)."""
    from pathlib import Path

    from .dependencies import create_client, get_env_variables

    # Get environment variables
    env_config = get_env_variables()

    # Create client
    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    client = create_client(env_config)

    # Test data - load config to get RAW table info
    # handler.py is at: modules/file_asset_hierarchy_extractor/functions/fn_dm_write_asset_hierarchy/handler.py
    # Need to go up 3 levels to get to file_asset_hierarchy_extractor module root
    module_root = Path(__file__).parent.parent.parent
    hierarchy_file = module_root / "results" / "asset_hierarchy.yaml"

    # Try to load RAW table info from config
    import yaml

    config_file = (
        module_root / "pipelines" / "ctx_write_asset_hierarchy_default.config.yaml"
    )
    raw_db = None
    raw_table_assets = None
    if config_file.exists():
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
            params = config.get("config", {}).get("parameters", {})
            raw_db = params.get("raw_db")
            raw_table_assets = params.get("raw_table_assets")

    data = {
        "logLevel": "DEBUG",
        "batch_size": 100,
        "dry_run": False,  # Set to False to actually write
        "_local_mode": True,  # Flag to indicate local execution (allows YAML fallback)
    }

    # Add RAW table info if available (preferred method)
    if raw_db:
        data["raw_db"] = raw_db
    if raw_table_assets:
        data["raw_table_assets"] = raw_table_assets

    # Add hierarchy_file as fallback if RAW is not available
    if hierarchy_file.exists():
        data["hierarchy_file"] = str(hierarchy_file)

    # Run handler
    print("Starting write asset hierarchy pipeline...")
    result = handle(data, client)

    if result["status"] == "succeeded":
        print("Pipeline completed successfully!")
    else:
        print(f"Pipeline failed: {result.get('message', 'Unknown error')}")

    return result


if __name__ == "__main__":
    try:
        result = run_locally()
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
