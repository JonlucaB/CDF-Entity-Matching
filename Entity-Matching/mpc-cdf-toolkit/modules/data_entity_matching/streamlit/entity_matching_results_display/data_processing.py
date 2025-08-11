import streamlit as st
import pandas as pd
from cognite.client import CogniteClient

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

CLIENT = get_cognite_client()

def get_database_names() -> list[str]:
    return CLIENT.raw.databases.list(limit=None).as_names()

def get_table_names(database_name: str) -> list[str]:
    return CLIENT.raw.tables.list(limit=None, db_name=database_name).as_names()

@st.cache_resource
def get_table_df(database_name, table_name) -> pd.DataFrame:
    return CLIENT.raw.rows.retrieve_dataframe(
        db_name=database_name,
        table_name=table_name,
        limit=None
    )

def write_table(database_name, table_name, df: pd.DataFrame) -> bool:
    st.write(st.session_state.table_df.columns)
    try:
        write_df=st.session_state.table_df.copy().reindex(df.index)[['score', 'external_id_source', 'external_id_target', 'space_source', 'space_target']]
        st.write(write_df)
        CLIENT.raw.rows.insert_dataframe(db_name=database_name, table_name=table_name, dataframe=df)
        st.success(f"Successfully uploaded {len(df)} rows to {table_name}")
        return True
    except Exception as e:
        st.error(e)
        return False