from pydantic import BaseModel, Field
from cognite.client import data_modeling as dm

class ViewId(BaseModel):
    """A config class for a CDF view id"""
    space: str = Field(None, description="Space of the view to retrieve")
    external_id: str = Field(None, description="External Id fo the view to retrieve")
    version: str = Field(None, description="Version of the view to retrieve")

    @property
    def as_view_id(self) -> dm.ViewId:
        """Returns the config object as a ViewId"""
        return dm.ViewId(space=self.space, external_id=self.external_id, version=self.version)

class Table(BaseModel):
    """A config class for a CDF table"""
    database_name: str = Field("db_entity_matching_job_result", description="The name of the database")
    table_name: str = Field(None, description="The name of the table")

class SupervisedConfig(BaseModel):
    """A config class for supervised learning"""
    id: int | None= Field(None, description="The existing model's ID - must supply if using supervised model as this is the model that will be refit if requested")
    external_id: str | None = Field(None, description="The existing model's externalId")

class ContextualizationModelConfig(BaseModel):
    """A config class for a contextualization model in CDF"""
    feature_type: str = Field("bigram", description="The feature type to use when matching tokens. Defaults to bigram")
    timeout: int = Field(120, description="The timout to set in minutes for api responses. Helpful for large matching jobs")

class ContextualizationConfig(BaseModel):
    """A config class for contextualization in CDF using entity matching"""
    true_match_threshold: float = Field(1.0, description="The score threshold for the model. The model will not return any matches that score lower than this value.")
    true_matches_table: Table = Field(None, description="The table to hold the true matches, which is used for supervised learning")
    write_true_matches: bool = Field(False, description="Flag for whether or not to wite the new true matches back to RAW (i.e replace existing training data)")
    num_matches: int = Field(1, description="The number of matches the model should predict for each source")
    score_threshold: float = Field(.8, description="The minimum score a match must have to be considered a match")
    supervised_config: SupervisedConfig | None = Field(None, description="The configuration for supervised learning")
    contextualization_model_config: ContextualizationModelConfig = Field(ContextualizationModelConfig(), description="The configuration for the model")
    match_result_table: Table = Field(None, description="The table to hold the match results")

class InstanceConfig(BaseModel):
    """A config class for querying instances in CDF"""
    instance_type: str = Field("node", description="Whether to query for nodes or edges. You can also pass a custom typed node (or edge class) inheriting from TypedNode (or TypedEdge).")
    view_id: ViewId = Field(None, description="The id of the view to pull instances from")
    instance_space: str = Field(None, description="Only return instances in the given space (or list of spaces).")
    fields_to_pull: list[str] = Field(["aliases"], description="What fields to include in the instance (auto populated with fields_to_contextualize)")
    fields_to_contextualize: list[str] | None = Field(None, description="What fields to contextualize with (creates a cross product between fields in target and source for match fields). If this field is left empty, every property will be used for contextualization")
    filter_tags: list[str] | None = Field(None, description="Tag values to filter on")

class Config(BaseModel):
    """The config class used for the standard_entity_matching function"""
    source_config: InstanceConfig = Field(None, description="The configuration for retrieving source instances from CDF.")
    target_config: InstanceConfig = Field(None, description="The configuration for retrieving target instances from CDF.")
    contextualization_config: ContextualizationConfig = Field(None, description="The configuration for the matching endpoint")
    log_level: str = Field("INFO", description="The log level")

    @property
    def is_supervised(self) -> bool:
        """Returns whether or not the configuration is for a supervised or unsupervised model """
        return self.contextualization_config.supervised_config is not None