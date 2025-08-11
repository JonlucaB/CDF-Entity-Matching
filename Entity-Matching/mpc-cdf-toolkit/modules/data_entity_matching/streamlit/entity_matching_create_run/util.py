import streamlit as st
import pandas as pd
from cognite.client import CogniteClient
import cognite.client.data_classes.data_modeling as dm
from typing import Optional, Any
import datetime
import json
from croniter import croniter
import hashlib

@st.cache_resource
def get_cognite_client() -> CogniteClient:
    """
    Initializes and returns a cached instance of the CogniteClient.
    Using @st.cache_resource ensures the client is created only once.
    This assumes credentials are stored as environment variables.
    """
    client = CogniteClient()
    client.config.timeout = 120
    return client

CLIENT=get_cognite_client()

def get_spaces() -> list[str]:
    return CLIENT.data_modeling.spaces.list(limit=None).as_ids()

def get_views(space: str) -> list[dm.ViewId]:
    return CLIENT.data_modeling.views.list(limit=None, space=space).as_ids()

def get_properties(input: str) -> list[str]:
    return [item.strip() for item in input.split(',')]

def get_databases() -> list[str]:
    return CLIENT.raw.databases.list(limit=None).as_names()

def get_tables(db_name: Optional[str]) -> list[str]:
    return CLIENT.raw.tables.list(
        limit=None,
        db_name=db_name
    ).as_names()

def get_models() -> list[int]:
    return CLIENT.entity_matching.list(limit=None).to_pandas()['id'].to_list()

def submit_config(config) -> bool:
    return False

def get_new_state_df(config: dict[str, Any], interval: str) -> pd.DataFrame:
    config_dump = json.dumps(config)
    encoded_string = str(datetime.datetime.now()).encode('utf-8') + config_dump[:16].encode('utf-8')
    md5_hash = hashlib.md5()
    md5_hash.update(encoded_string)
    full_hex_hash = md5_hash.hexdigest()
    new_id = full_hex_hash



    df=pd.DataFrame({
        "ID" : new_id,
        "STATUS" : "NEW",
        "MATCHING_JOB_ID" : None,
        "MODEL_ID" : config["contextualization_config"]["supervised_config"]["id"] if config["contextualization_config"]["supervised_config"] else None,
        "SOURCE_CREATED_TIME" : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SOURCE_UPDATED_TIME" : None,
        "SOURCE_CREATED_USER" : "STREAMLIT",
        "SOURCE_UPDATED_USER" : None,
        "INTERVAL" : interval,
        "ACTIVE" : False,
        "CONFIG" : config_dump
    }, index=[new_id])

    return df

def add_state(state: pd.DataFrame) -> bool:
    try:
        CLIENT.raw.rows.insert_dataframe(
            db_name="db_entity_matching_state_store",
            table_name="entity_matching_job_states",
            dataframe=state
        )

        return True
    except Exception as e:
        st.error(f"Failed to upload new state:{e}")
        return False
    
def check_cron(exp: str) -> bool:
    return croniter.is_valid(exp)