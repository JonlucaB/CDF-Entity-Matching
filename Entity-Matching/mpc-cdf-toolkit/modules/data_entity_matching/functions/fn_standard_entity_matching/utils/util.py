from __future__ import annotations
from cognite.client import data_modeling as dm, CogniteClient as client
from pydantic import BaseModel, field_validator, model_validator
from collections.abc import MutableSequence, Sequence
from datetime import datetime
from enum import StrEnum
from typing import Any, Optional
from utils.config import Config
import math
from utils.Constants import *
import pandas as pd
import numpy as np
import itertools
from croniter import croniter
import json

def should_cron_run(cron_string: str, last_run_iso: str) -> bool:
    """
    Checks if a cron schedule should have run at least once since the last
    successful run.

    Args:
        cron_string: A standard cron string (e.g., '0 0 * * *' for midnight daily).
        last_run_iso: A datetime string in ISO 8601 format (e.g., '2025-07-31T16:00:00Z').

    Returns:
        True if the cron schedule has a next execution time that is after
        the provided last_run_iso. False otherwise.
    """

    if cron_string is None or cron_string == "" or not croniter.is_valid(cron_string):
        return False

    try:
        # Parse the ISO datetime string into a datetime object
        last_run_time = datetime.fromisoformat(last_run_iso)

        # Create a croniter object with the cron string and the last run time
        itr = croniter(cron_string, last_run_time)

        # Get the next scheduled execution time
        next_run_time = itr.get_next(datetime)

        # Compare the next run time to the last run time
        # This will be True if there has been a scheduled run since last_run_time
        return datetime.now() > next_run_time

    except Exception as e:
        print(f"Error processing cron string or datetime: {e}")
        return False

class EntityMatchingStatus(StrEnum):
    # State store has been created and added to state table
    NEW = "NEW"
    
    # Matching job has been created, handed off to CDF
    PROCESSING = "PROCESSING"

    # Match results have been written to RAW
    FINALIZED = "FINALIZED"

    # Something went wrong with this one
    ERROR = "ERROR"

class MatchingState(BaseModel):
    matching_status: EntityMatchingStatus
    id: str | None = None
    matching_job_id: int | None = None
    model_id: int | None = None
    source_created_time: str = None
    source_updated_time: str | None = None
    source_created_user: str | None = "fn_standard_entity_matching_launch"
    source_updated_user: str | None= "fn_standard_entity_matching_launch"
    active: bool = True
    config: Config | None = None
    interval: str | None = None

    @field_validator("matching_status", mode="before")
    def load_matching_status(cls, value: Any) -> Any:
        if isinstance(value, (EntityMatchingStatus)):
            return value

    @field_validator("id", mode="before")
    def load_id(cls, value: Any) -> Any:
        if value is not None:
            if isinstance(value, str):
                return value
            else:
                return value
            
    @field_validator("model_id", mode="before")
    def load_model_id(cls, value: Any) -> Any:
        if value is None:
            return None
        elif math.isnan(value):
            return None
        else:
            return value

    @field_validator("matching_job_id", mode="before")
    def load_matching_job_id(cls, value: Any) -> Any:
        if value is not None:
            if isinstance(value, str):
                return int(value)
            elif math.isnan(value):
                return None
            else:
                return value

    @field_validator("source_created_time", mode="before")
    def load_source_created_time(cls, value: Any) -> Any:
        if value is not None:
            return value
    
    @field_validator("source_created_user", mode="before")
    def load_source_created_user(cls, value: Any) -> Any:
        if value is None:
            return "NEW STATE"
        else:
            return value
    
    @field_validator("source_updated_user", mode="before")
    def load_(cls, value: Any) -> Any:
        return value
    
    @field_validator("active", mode="before")
    def load_active(cls, value: Any) -> Any:
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return bool(value)
        else:
            return True

    @field_validator("config", mode="before")
    def load_config(cls, value: Any) -> Any:
        if isinstance(value, Config):
            return value
        elif isinstance(value, dict):
            return Config(**value)
        elif isinstance(value, str):
            try:
                raw_dict=json.loads(value)
                return Config(**raw_dict)
            except Exception as e:
                return None
        else:
            raise ValueError("Config must be defined and consumable for states.")


    @classmethod
    def from_row(cls, row: pd.Series) -> "MatchingState":
        """Creates a MatchingState from the dataframe row"""

        properties=row.to_dict()
        
        try:
            matching_status=EntityMatchingStatus(properties.get(STATE_STATUS, None))
            id=properties['ID']
            matchingJobId=properties.get(STATE_MATCHING_JOB_ID, None)
            source_created_time=properties.get(STATE_SOURCE_CREATED_TIME, None)
            source_updated_time=properties.get(STATE_SOURCE_UPDATED_TIME, None)
            source_created_user=properties.get(STATE_SOURCE_CREATED_USER, None)
            source_updated_user=properties.get(STATE_SOURCE_UPDATED_USER, None)
            config=properties.get(STATE_CONFIG, None)
            model_id=properties.get(STATE_MODEL_ID, None)
            interval=properties.get(STATE_INTERVAL, None)
        except Exception as e:
            raise e

        return cls(
            matching_status=matching_status,
            id=id,
            matching_job_id=matchingJobId,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            config=config,
            model_id=model_id,
            interval=interval
        )

    def to_pandas(self) -> pd.DataFrame:
        properties=pd.DataFrame({
            STATE_STATUS : self.matching_status,
            STATE_ID : self.id,
            STATE_MODEL_ID : self.model_id,
            STATE_MATCHING_JOB_ID : self.matching_job_id,
            STATE_SOURCE_CREATED_TIME : self.source_created_time,
            STATE_SOURCE_UPDATED_TIME : self.source_updated_time,
            STATE_SOURCE_CREATED_USER : self.source_created_user,
            STATE_SOURCE_UPDATED_USER: self.source_updated_user,
            STATE_INTERVAL : self.interval,
            STATE_CONFIG : self.config.model_dump_json()
        }, index=[self.id])

        return properties
    

# Assuming to_camel is a utility function, defining it here for completeness
def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])

class Entity(BaseModel, alias_generator=to_camel, extra="allow", populate_by_name=True):
    node_id: dm.NodeId
    view: Optional[dm.ViewId] # Made view optional for flexibility in from_row
    standardized_properties: dict[str, str]
    name_by_alias: dict[str, str]

    @model_validator(mode="before")
    def pack_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """To a dictionary of properties, adds every property that starts with 'prop' to a new key called 'standardized_properties'"""
        if "standardized_properties" in values or "standardizedProperties" in values:
            return values
        standardized_properties: dict[str, str]={}
        for key in list(values.keys()):
            if key.startswith("prop"):
                standardized_properties[key]=values.pop(key)
        return {**values, "standardized_properties": standardized_properties}
    
    @field_validator("node_id", mode="before")
    def load_node_id(cls, value: Any) -> Any:
        """Loads the node id from a dictionary, tuple, or NodeId"""
        if isinstance(value, (dict, tuple, dm.NodeId)):
            return dm.NodeId.load(value)
        elif isinstance(value, str):
            return dm.NodeId.load(json.loads(value))
        return value
    
    @field_validator("view", mode="before")
    def load_view_id(cls, value: Any) -> Any:
        """Loads the view id from a dictionary, tuple, or viewId"""
        if isinstance(value, (dict, tuple, dm.ViewId)):
            return dm.ViewId.load(value)
        elif isinstance(value, str):
            return dm.ViewId.load(json.loads(value))
        return value
    
    @field_validator("name_by_alias", mode="before")
    def load_name_by_alias(cls, value: Any) -> Any:
        """Loads the name_by_alias from a string or list"""
        if isinstance(value, str):
            return json.loads(value)
        else:
            return value

    @classmethod
    def from_node(cls, node: dm.Node) -> "Entity":
        """Creates an entity with standardized properties from a Node object and a list of properties"""
        if not node.properties:
            raise ValueError(f"Node {node.as_id()} does not have properties")
        view_id, node_properties=next(iter(node.properties.items()))
        standardized_properties: dict[str, str]={}
        name_by_alias: dict[str, str]={}

        for no, prop in enumerate(node_properties.keys()):
            alias=f"prop{no}"
            standardized_properties[alias]=str(node_properties[prop]).replace('[', "").replace(']', "")
            name_by_alias[alias]=prop

        return cls(
            node_id=node.as_id(),
            view=view_id,
            standardized_properties=standardized_properties,
            name_by_alias=name_by_alias
        )

    def to_series(self) -> pd.Series:
        """Converts the entity into a series. If properties is None, then the series is just the space, externalId, and name"""
        properties_dict: dict[str, Any]={}

        for st_name, name in self.name_by_alias.items():
            properties_dict.update({name : self.standardized_properties.get(st_name)})

        # The 'name' here should come from properties_dict if 'name' is in properties, otherwise 'UNKNOWN'
        name_from_props = properties_dict.get('name', 'UNKNOWN')

        return pd.Series({
            'space' : self.node_id.space,
            'external_id' : self.node_id.external_id,
            'name' : name_from_props,
            **properties_dict
        })

    def dump(self) -> dict[str, Any]:
        """Dumps the entity into JSON format"""
        return {
            "nodeId": json.dumps(self.node_id.dump()),
            "externalId": self.node_id.external_id,
            "view": json.dumps(self.view.dump()), # Handle optional view
            **self.standardized_properties,
            "nameByAlias": json.dumps(self.name_by_alias),
        }
    
    def explode(self) -> Entity | EntityList | None:
        """Explode every property that is a collection and dump it into json"""
        entity_series=self.to_series()

        entity_df=entity_series.to_frame().T
        exploded_entity_df=explode_dataframe_collections(
            df=entity_df,
            columns_to_check=list(self.name_by_alias.values()) # Use actual original property names for check
        )
        # Re-map standardized properties to original names for easy access in from_row if needed
        # Or ensure from_row can handle the propX aliases directly from standardized_properties
        
        # For simplicity, let's keep standardized_properties and name_by_alias as they are
        # and ensure from_row correctly reconstructs from the exploded data.
        # The `exploded_entity_df` will have the `propX` as columns if those were exploded.

        # Ensure `name_by_alias` is carried over correctly for each exploded row
        exploded_entity_df['original_name_by_alias'] = [self.name_by_alias] * len(exploded_entity_df)
        exploded_entity_df['original_view'] = [self.view] * len(exploded_entity_df)


        num_exploded_entities=len(exploded_entity_df)
        if num_exploded_entities == 1:
            row=exploded_entity_df.iloc[0]
            # When creating a new Entity from a row, we need the original property names
            # and the values from the row.
            # `from_row` should take the original property names (values of name_by_alias)
            # and the row, then standardize them.
            original_prop_names = list(row['original_name_by_alias'].values())
            return self.from_row(row, original_prop_names, row['original_view'])
        elif num_exploded_entities > 1:
            new_list=EntityList()
            for i, entity_row in exploded_entity_df.iterrows():
                original_prop_names = list(entity_row['original_name_by_alias'].values())
                created_entity = self.from_row(entity_row, original_prop_names, entity_row['original_view'])
                if created_entity:
                    new_list.append(created_entity)
            
            return new_list
        else:
            # If no entities after explosion (e.g., empty dataframe), return None
            return None

    @classmethod
    def from_row(cls, row: pd.Series, original_property_names: list[str], view_id: Optional[dm.ViewId] = None) -> "Entity" | None:
        """Creates an entity with standardized properties from a row (series) object and a list of original property names."""
        node_id = dm.NodeId(row['space'], row['external_id'])
        
        standardized_properties: dict[str, str]={}
        name_by_alias: dict[str, str]={}

        for no, prop_name in enumerate(original_property_names):
            if prop_name in row and pd.notnull(row[prop_name]):
                alias=f"prop{no}"
                standardized_properties[alias]=str(row[prop_name])
                name_by_alias[alias]=prop_name

        # Ensure that `view_id` is a `dm.ViewId` object or None
        if isinstance(view_id, (dict, tuple)):
            view_id = dm.ViewId.load(view_id)

        return cls(
            node_id=node_id,
            view=view_id,
            standardized_properties=standardized_properties,
            name_by_alias=name_by_alias
        )


class EntityList(list, MutableSequence[Entity]):
    """A list of entities"""
    @property
    def unique_properties(self) -> set[str]:
        """Gets the set of unique properties of all the entities in the list"""
        all_standardized_keys = set()
        for entity in self:
            all_standardized_keys.update(entity.standardized_properties.keys())
        return all_standardized_keys
    
    @property
    def alias_by_names(self) -> dict[str, str]:
        """Gets the set of mappings from alias to property name"""
        all_alias_by_names=dict[str, str]()

        for entity in self:
            all_alias_by_names.update(entity.name_by_alias)
        return all_alias_by_names
    
    @classmethod
    def from_nodes(cls, nodes: Sequence[dm.Node]) -> "EntityList":
        """Generates the entity list off of a sequence of nodes and a list of properties to include"""
        return cls([Entity.from_node(node) for node in nodes if Entity.from_node(node) is not None])

    def dump(self) -> list[dict[str, Any]]:
        """Dumps the entity list in JSON format"""
        return [entity.dump() for entity in self]
    
    def explode_and_dump(self) -> list[dict[str, Any]]:
        """Explodes the entity list for collection properties and dumps them into a dictionary"""
        new_list: list[dict[str, Any]]=[]

        for entity in self:
            exploded_entity=entity.explode()
            if isinstance(exploded_entity, Entity):
                new_list.append(exploded_entity.dump())
            elif isinstance(exploded_entity, EntityList):
                for exp_ent_el in exploded_entity:
                    new_list.append(exp_ent_el.dump())
            elif exploded_entity is None:
                continue
            else:
                raise ValueError("Unknown type encountered while exploding and dumping entity list")
        return new_list # Return the new_list

    def property_product(self, other: "EntityList", source_match_properties: list[str]=None, other_match_properties: list[str]=None) -> list[tuple[str, str]]:
        """Gets the cross product of all unique properties in Source and Target"""
        if source_match_properties is not None and other_match_properties is not None:
            self_match_properties=[]
            target_match_properties=[]

            self_alias_mappings=self.alias_by_names
            other_alias_mappings=other.alias_by_names

            for alias, name in self_alias_mappings.items():
                if name in source_match_properties:
                    self_match_properties.append(alias)

            for alias, name in other_alias_mappings.items():
                if name in other_match_properties:
                    target_match_properties.append(alias)

            return [(source, target) for source, target in itertools.product(self_match_properties, target_match_properties)]
        else:
            return [
                (source, target) for source, target in itertools.product(self.unique_properties, other.unique_properties)
            ]
    
    
class MatchItem(BaseModel, alias_generator=to_camel):
    """A class representing a match"""
    score: float
    target: Entity

    def to_series(self) -> pd.Series:
        target_series=self.target.to_series()
        target_series['score'] = self.score

        return target_series

class MatchResult(BaseModel, alias_generator=to_camel):
    """A class representing a match result for a source entity"""
    source: Entity
    matches: list[MatchItem]

    @property
    def best_match(self) -> MatchItem | None:
        """Gets the best match of all the matches in the result"""
        return max(self.matches, key=lambda match: match.score) if self.matches else None
    
    def to_pandas(self) -> pd.DataFrame:
        """Returns a dataframe of the MatchResult object"""

        if self.matches is []:
            # Handle case where there's no best match; return an empty DataFrame or raise specific error
            # Current code raises Exception, which might be desired.
            # If an empty DF is desired, return pd.DataFrame(columns=[...])
            raise Exception(f"No match found for source: {self.source.dump()}")
        else:
            # Ensure top_match.to_series returns a Series, and then convert to DataFrame
            # before merging. Using .to_frame().T to make it a single-row DataFrame.
            # top_match_df = top_match.to_series(properties).to_frame().T
            targets_df = pd.DataFrame([match.to_series() for match in self.matches])
            source_df = self.source.to_series().to_frame().T

            # Need to ensure columns are distinct before merging if names overlap
            # The suffixes '_source' and '_target' should handle this.
            return pd.merge(
                left=source_df,
                right=targets_df,
                how='outer',
                left_index=True, # Merge on index as they are single-row DFs
                right_index=True, # Merge on index
                suffixes=('_source', '_target')
            )

    @classmethod
    def load(cls, data: dict[str, Any]) -> "MatchResult":
        """Loads the MatchResult from a dictionary of {'source' : <Entity>, 'matches' : <list[MatchItem]>}"""
        # Pydantic's model_validate can handle nested models automatically
        return cls.model_validate(data)
    
class MatchResultList(list, MutableSequence[MatchResult]):
    def to_pandas(self, explode_properties=False) -> pd.DataFrame:
        """Returns a dataframe of the MatchResultList"""
        # explode properties will eventually explode a metadata field or something. Not implemented yet
        if self == []:
            return pd.DataFrame()
        try:
            # Ensure each result.to_pandas() returns a DataFrame that can be concatenated
            dfs=[result.to_pandas() for result in self if len(result.matches) > 0]
            if len(dfs) == 0:
                return pd.DataFrame()

            df=pd.concat(objs=dfs)
            df.dropna(axis=0, subset=['external_id_target'])
            df['source_target_external_id']=df.apply(lambda r: f"{r['external_id_source']}_{r['external_id_target']}", axis=1)
            df.set_index('source_target_external_id', drop=True, inplace=True)
            return df[~df.index.duplicated(keep='first')]
        except Exception as e:
            raise(e)
    
def explode_dataframe_collections(df: pd.DataFrame, columns_to_check: list[str]) -> pd.DataFrame:
    """     
    Explodes the DataFrame for every column in 'columns_to_check' that contains collections (lists, tuples, sets).     
    Args:         df (pd.DataFrame): The input DataFrame.         
    columns_to_check (List[str]): A list of column names to iterate through.                                       
    For each column, it checks if it contains collections                                       
    and applies df.explode() if it does.     
    Returns:         pd.DataFrame: The DataFrame with the specified collection columns exploded.                       
    A copy of the DataFrame is used to avoid modifying the original in-place.     
    """
    wrangled_df=df.copy()
 
    for col_name in columns_to_check:
        if col_name in wrangled_df.columns:
            first_non_null_val=None 
            if not wrangled_df[col_name].dropna().empty:
                first_non_null_val=wrangled_df[col_name].dropna().iloc[0]

            if isinstance(first_non_null_val, (list, tuple, set)):
                print(f"DEBUG: Found collection in column '{col_name}'. Explodin' it out!")
                wrangled_df=wrangled_df.explode(col_name)
 
    return wrangled_df.replace([-np.inf, np.inf, np.nan, math.nan], '')

def elapsed_time(start: datetime) -> str:
    """Prints the time elapsed since start in a readable format"""
    td=datetime.now()-start
    days=td.days
    total_seconds_in_day=td.seconds
    hours, remainder_seconds=divmod(total_seconds_in_day, 3600)
    minutes, seconds=divmod(remainder_seconds, 60)

    parts=[]
    if days != 0:
        parts.append(f"{days} day{'s' if abs(days) != 1 else ''}")
    if hours != 0:
        parts.append(f"{hours} hour{'s' if abs(hours) != 1 else ''}")
    if minutes != 0:
        parts.append(f"{minutes} minute{'s' if abs(minutes) != 1 else ''}")
    if seconds != 0:
        parts.append(f"{seconds} second{'s' if abs(seconds) != 1 else ''}")
    
    if not parts and td.microseconds != 0:
        parts.append(f"{td.microseconds} microsecond{'s' if abs(td.microseconds) != 1 else ''}")
    
    if not parts:
        return "0 seconds"
    
    return ", ".join(parts)