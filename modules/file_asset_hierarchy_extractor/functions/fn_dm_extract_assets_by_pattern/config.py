"""
Configuration models for extract annotation tags.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, List, Literal, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field


class Parameters(BaseModel):
    """Parameters for extract annotation tags pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    run_all: bool = Field(True, description="Run all files")
    overwrite: bool = Field(False, description="Overwrite existing results")
    initialize_state: bool = Field(
        False,
        description="Only query for new files and initialize state, skip processing",
    )
    raw_db: str = Field(..., description="ID of the raw database")
    raw_table_state: str = Field(..., description="ID of the state table in RAW")
    raw_table_results: Optional[str] = Field(
        None,
        description="ID of the results table in RAW (optional, defaults to state table)",
    )
    results_field: str = Field(
        "results", description="Field name for results in the state/results table"
    )


class Config(BaseModel):
    """Configuration model for extract annotation tags extraction pipeline."""

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
