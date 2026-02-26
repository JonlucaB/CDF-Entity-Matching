"""
Configuration models for create asset hierarchy.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field


class Parameters(BaseModel):
    """Parameters for create asset hierarchy pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    run_all: bool = Field(True, description="Run all files")
    overwrite: bool = Field(False, description="Overwrite existing results")
    raw_db: str = Field(..., description="ID of the RAW database")
    raw_table_state: str = Field(
        ..., description="ID of the state table in RAW (contains results)"
    )
    results_field: str = Field(
        "results", description="Field name for results in state table"
    )
    raw_table_assets: str = Field(
        ..., description="ID of the assets table in RAW for storing generated hierarchy"
    )
    output_file: Optional[str] = Field(
        None,
        description="Output file path for asset hierarchy YAML (only used when running locally)",
    )
    space: str = Field("sp_enterprise_schema", description="Instance space for assets")
    include_resource_type: bool = Field(
        False, description="Include resourceType (tag_class_name) as intermediate level"
    )
    include_resource_subtype: bool = Field(
        False,
        description="Include resourceSubType (equipment_class_name) as intermediate level",
    )
    include_resource_subsubtype: bool = Field(
        False,
        description="Include resourceSubSubType (equipment_subclass_name) as intermediate level",
    )
    include_resource_variant: bool = Field(
        False,
        description="Include resourceVariant (equipment_variant_name) as intermediate level",
    )
    logLevel: str = Field("INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")
    limit: Optional[int] = Field(
        -1, description="Maximum number of files to process (-1 for no limit)"
    )
    batch_size: Optional[int] = Field(
        None, description="Number of files to process per batch (None for no batching)"
    )
    tag_prefix_whitelist: Optional[List[str]] = Field(
        None,
        description="Optional list of tag text prefixes. If set, extracted tags whose 'text' does not start with any of these prefixes have their confidence set to 0.0. If not set or empty, no filtering is applied.",
    )
    tag_blacklist: Optional[List[str]] = Field(
        None,
        description="Optional list of strings. If set, extracted tags whose 'text' contains any of these values have their confidence set to 0.0. If not set or empty, no filtering is applied.",
    )


class Config(BaseModel):
    """Configuration model for create asset hierarchy extraction pipeline."""

    externalId: str
    config: Dict[str, Any]

    @property
    def parameters(self) -> Parameters:
        """Get parameters from config."""
        params = self.config.get("parameters", {})
        return Parameters.model_validate(params)

    @property
    def data(self) -> Dict[str, Any]:
        """Get data section from config."""
        return self.config.get("data", {})


def load_config_parameters(
    client: CogniteClient, function_data: Dict[str, Any]
) -> Config:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError(
            "Missing key 'ExtractionPipelineExtId' in input data to the function"
        )

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(
                f"No config found for extraction pipeline: {pipeline_ext_id!r}"
            )
    except CogniteAPIError:
        raise RuntimeError(
            f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}"
        )

    config_dict = yaml.safe_load(raw_config.config)
    config_dict["externalId"] = pipeline_ext_id
    return Config.model_validate(config_dict)
