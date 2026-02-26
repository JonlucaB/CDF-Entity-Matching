"""
CDF Adapter for Key Extraction Engine

This module provides adapters to convert CDF extraction pipeline configurations
to the format expected by the KeyExtractionEngine, enabling compatibility with
CDF workflow formats while maintaining existing functionality.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Import CDF Config model internally (not exposed to users)
try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    Config = None
    load_config_parameters = None


def convert_cdf_config_to_engine_config(cdf_config: Any) -> Dict[str, Any]:
    """
    Convert CDF Config (Pydantic model) to the dict format expected by KeyExtractionEngine.

    Args:
        cdf_config: CDF Config object from config.py (Pydantic model)

    Returns:
        Dict format compatible with KeyExtractionEngine
    """
    engine_config = {
        "extraction_rules": cdf_config.data.extraction_rules,  # Pass directly now
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 1000},
        "field_selection_strategy": cdf_config.data.field_selection_strategy,
    }

    return engine_config


def load_config_from_yaml(config_path: str, validate: bool = True) -> Dict[str, Any]:
    """
    Load CDF extraction pipeline config from YAML file and convert to engine format.

    This is a convenience function that loads a YAML config file, optionally validates it
    using the CDF Config Pydantic model, and converts it to the engine format.

    Args:
        config_path: Path to the YAML config file
        validate: Whether to validate using Pydantic model (default: True)

    Returns:
        Engine config dictionary compatible with KeyExtractionEngine

    Raises:
        ImportError: If CDF config models are not available and validate=True
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load YAML
    with open(config_file) as f:
        yaml_data = yaml.safe_load(f)

    # Extract config section
    config_data = yaml_data.get("config", yaml_data)

    # Option 1: Validate using Pydantic model (preferred if available)
    if validate and CDF_CONFIG_AVAILABLE:
        try:
            cdf_config = Config.model_validate(config_data)
            return convert_cdf_config_to_engine_config(cdf_config)
        except Exception as e:
            raise ValueError(f"Invalid config structure: {e}") from e

    # Option 2: Direct conversion without Pydantic validation (fallback)
    if not validate or not CDF_CONFIG_AVAILABLE:
        logger.warning(
            "Loading config without Pydantic validation not supported in simplified adapter."
        )
        raise NotImplementedError(
            "Direct YAML loading without Pydantic validation has been removed. "
            "Please ensure CDF Config models are available."
        )

    raise ImportError(
        "CDF Config models not available and validation is required. "
        "fn_dm_key_extraction module is required for loading YAML configs with validation."
    )


def load_config_from_cdf(client: Any, pipeline_ext_id: str) -> Dict[str, Any]:
    """
    Load CDF extraction pipeline config from CDF and convert to engine format.

    Args:
        client: CogniteClient instance
        pipeline_ext_id: External ID of the extraction pipeline

    Returns:
        Engine config dictionary compatible with KeyExtractionEngine

    Raises:
        ImportError: If CDF config models are not available
        RuntimeError: If config cannot be retrieved from CDF
    """
    if not CDF_CONFIG_AVAILABLE:
        raise ImportError(
            "CDF Config models not available. "
            "fn_dm_key_extraction module is required for loading from CDF."
        )

    function_data = {"ExtractionPipelineExtId": pipeline_ext_id}
    cdf_config = load_config_parameters(client, function_data)
    return convert_cdf_config_to_engine_config(cdf_config)
