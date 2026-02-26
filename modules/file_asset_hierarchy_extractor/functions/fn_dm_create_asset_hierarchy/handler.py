"""
CDF Handler for Create Asset Hierarchy

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


def handle(
    data: Dict[str, Any], client: CogniteClient = None
) -> Dict[str, Any]:  # noqa: ARG001
    """
    CDF-compatible handler function for create asset hierarchy.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline (optional)
            - locations: List of location dictionaries (required, or provided via config)
            - tags_file: Path to extracted tags CSV file (optional if tags provided)
            - tags: List of tag dictionaries (optional if tags_file provided)
            - output_file: Path to output YAML file (optional)
            - space: Instance space for assets (default: sp_enterprise_schema)
            - include_resource_type: Include resourceType as intermediate level (default: False)
            - include_resource_subtype: Include resourceSubType as intermediate level (default: False)
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required if using CDF config loading)

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

        logger.info(f"Starting create asset hierarchy with loglevel = {loglevel}")

        # Load configuration from CDF extraction pipeline
        # Check if _cdf_config is already provided (e.g., from run_locally)
        cdf_config = data.get("_cdf_config")
        if (
            cdf_config is None
            and CDF_CONFIG_AVAILABLE
            and client
            and "ExtractionPipelineExtId" in data
        ):
            pipeline_ext_id = data["ExtractionPipelineExtId"]
            logger.info(f"Loading config from extraction pipeline: {pipeline_ext_id}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

            # Store CDF config for use in pipeline
            data["_cdf_config"] = cdf_config

        # Extract parameters from CDF config and merge into data if config is available
        if cdf_config is not None:
            config_data = cdf_config.data
            config_params = cdf_config.parameters

            # Handle locations - must be provided directly in config data section
            if "locations" not in data and "locations" in config_data:
                # Locations are provided directly in config
                locations_yaml = config_data["locations"]
                # Get hierarchy_levels from config_data if available
                hierarchy_levels = config_data.get("hierarchy_levels")
                # Convert YAML structure to flat list format using location_utils
                from .utils.location_utils import convert_locations_dict_to_flat_list

                locations = convert_locations_dict_to_flat_list(
                    locations_yaml, hierarchy_levels
                )
                data["locations"] = locations
                # Also store hierarchy_levels in data for use in pipeline
                if hierarchy_levels:
                    data["hierarchy_levels"] = hierarchy_levels
            if "tags_file" not in data and "tags_file" in config_data:
                data["tags_file"] = config_data["tags_file"]

            # Read from parameters section (moved from data section)
            if "output_file" not in data and config_params.output_file:
                data["output_file"] = config_params.output_file
            if (
                "pattern_config_path" not in data
                and hasattr(config_params, "pattern_config_path")
                and config_params.pattern_config_path
            ):
                data["pattern_config_path"] = config_params.pattern_config_path
            if "space" not in data:
                data["space"] = config_params.space
            if "include_resource_type" not in data:
                data["include_resource_type"] = config_params.include_resource_type
            if "include_resource_subtype" not in data:
                data[
                    "include_resource_subtype"
                ] = config_params.include_resource_subtype
            if "include_resource_subsubtype" not in data:
                data[
                    "include_resource_subsubtype"
                ] = config_params.include_resource_subsubtype
            if "include_resource_variant" not in data:
                data[
                    "include_resource_variant"
                ] = config_params.include_resource_variant
            if "tag_prefix_whitelist" not in data and getattr(
                config_params, "tag_prefix_whitelist", None
            ):
                data["tag_prefix_whitelist"] = config_params.tag_prefix_whitelist
            if "tag_blacklist" not in data and getattr(
                config_params, "tag_blacklist", None
            ):
                data["tag_blacklist"] = config_params.tag_blacklist
            if "logLevel" not in data:
                data["logLevel"] = config_params.logLevel

        # Get client if available (needed for loading assets from RAW)
        if client is None and CDF_AVAILABLE:
            from .dependencies import create_client, get_env_variables

            try:
                env_vars = get_env_variables()
                client = create_client(env_vars)
            except Exception as e:
                logger.warning(f"Could not create client: {e}")

        # Call pipeline function
        from .pipeline import create_asset_hierarchy

        create_asset_hierarchy(
            client=client,
            logger=logger,
            data=data,
        )

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Create asset hierarchy pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing."""
    from pathlib import Path

    import yaml

    # Test data - load locations from config file
    # handler.py is at: modules/file_asset_hierarchy_extractor/functions/fn_dm_create_asset_hierarchy/handler.py
    # Need to go up 3 levels to get to file_asset_hierarchy_extractor module root
    script_dir = Path(__file__).parent.parent.parent
    config_file = (
        script_dir / "pipelines" / "ctx_create_asset_hierarchy_default.config.yaml"
    )
    output_file = script_dir / "results" / "asset_hierarchy.yaml"

    # Create client for loading assets from RAW
    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    from .dependencies import create_client, get_env_variables

    env_vars = get_env_variables()
    client = create_client(env_vars)

    # Load config from local YAML file for local testing
    if config_file.exists():
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
            config_data = config.get("config", {})
            parameters = config_data.get("parameters", {})
            data_section = config_data.get("data", {})

        # Create a mock CDF config structure
        class MockCDFConfig:
            def __init__(self):
                self.parameters = type("obj", (object,), parameters)()
                self.data = data_section

        # Convert locations YAML structure to flat list if needed
        locations_data = data_section.get("locations")
        hierarchy_levels = data_section.get("hierarchy_levels")
        locations = None
        if locations_data:
            from .utils.location_utils import convert_locations_dict_to_flat_list

            locations = convert_locations_dict_to_flat_list(
                locations_data, hierarchy_levels
            )

        # Use CDF format - assets will be loaded from RAW
        # Read from parameters section (moved from data section)
        data = {
            "logLevel": parameters.get("logLevel", "DEBUG"),
            "ExtractionPipelineExtId": "ctx_create_asset_hierarchy_default",
            "locations": locations,
            "hierarchy_levels": hierarchy_levels,  # Pass hierarchy_levels to pipeline
            "output_file": parameters.get("output_file") or str(output_file),
            "space": parameters.get("space", "sp_enterprise_schema"),
            "include_resource_type": parameters.get("include_resource_type", False),
            "include_resource_subtype": parameters.get(
                "include_resource_subtype", False
            ),
            "include_resource_subsubtype": parameters.get(
                "include_resource_subsubtype", False
            ),
            "include_resource_variant": parameters.get(
                "include_resource_variant", False
            ),
            "_cdf_config": MockCDFConfig(),  # Mock config for local testing
            "_local_mode": True,  # Flag to indicate local execution (write YAML, not RAW)
        }
    else:
        raise FileNotFoundError(f"Config file not found: {config_file}")

    # Run handler
    print("Starting create asset hierarchy pipeline...")
    result = handle(data, client=client)

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
