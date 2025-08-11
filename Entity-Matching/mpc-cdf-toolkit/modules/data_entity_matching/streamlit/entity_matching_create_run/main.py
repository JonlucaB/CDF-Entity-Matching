import streamlit as st
from util import *
import time

default_config = {
    "source_config": {
        "instance_type": "node",
        "view_id": {
            "space": None,
            "external_id": None,
            "version": None
        },
        "instance_space": None,
        "fields_to_pull": [
            "fields"
        ],
        "fields_to_contextualize": [
            "fields"
        ],
        "filter_tags" : None
    },
    "target_config": {
        "instance_type": "node",
        "view_id": {
            "space": None,
            "external_id": None,
            "version": None
        },
        "instance_space": "space",
        "fields_to_pull": [
            "fields"
        ],
        "fields_to_contextualize": [
            "fields"
        ],
        "filter_tags" : None
    },
    "contextualization_config": {
        "num_matches": 1,
        "score_threshold": 0.5,
        "write_true_matches": False,
        "true_match_threshold": 0.9,
        "true_matches_table": {
            "database_name": "db_entity_matching_job_result",
            "table_name": "this_true_table_does_not_exist"
        },
        "supervised_config": {
            "id": 0
        },
        "contextualization_model_config": {
            "feature_type": "bigram",
            "timeout": 120
        },
        "match_result_table": {
            "database_name": "db_entity_matching_job_result",
            "table_name": "this_table_does_not_exist"
        }
    },
    "log_level": "INFO"
}


st.header("Source Configuration")
with st.container(border=True):
  config_data=default_config

  st.subheader("Source Config Details")
  config_data["source_config"]["instance_type"] = st.text_input(
      "Source Instance Type",
      value=config_data["source_config"]["instance_type"],
      key="src_instance_type"
  )

  config_data["source_config"]["view_id"]["space"] = st.selectbox(
      label="Source View ID Space",
      options=get_spaces(),
      key="src_view_id_space"
  )

  if config_data["source_config"]["view_id"]["space"] != None:
    st.markdown("#### Source View ID")

    source_view_id = st.selectbox(
      label="Select the View",
      options=get_views(config_data["source_config"]["view_id"]["space"]),
      format_func=(lambda id: id.external_id),
      key="src_view_id"
    )

    if source_view_id is not None:
        config_data["source_config"]["view_id"]["external_id"]=source_view_id.external_id
        config_data["source_config"]["view_id"]["version"]=source_view_id.version

  config_data["source_config"]["instance_space"] = st.selectbox(
      label="Source Instance Space",
      options=get_spaces(),
      key="src_instance_space"
  )

  fields_to_pull_src = st.text_area(
      "Source Fields to Pull (comma-separated)",
      value=", ".join(config_data["source_config"]["fields_to_pull"]),
      key="src_fields_to_pull"
  )
  config_data["source_config"]["fields_to_pull"] = [
      f.strip() for f in fields_to_pull_src.split(',') if f.strip()
  ]

  fields_to_contextualize_src = st.text_area(
      "Source Fields to Contextualize (comma-separated)",
      value=", ".join(config_data["source_config"]["fields_to_contextualize"]),
      key="src_fields_to_contextualize"
  )

  config_data["source_config"]["fields_to_contextualize"] = [
      f.strip() for f in fields_to_contextualize_src.split(',') if f.strip()
  ]

  if st.checkbox("Add tag filters", value=False, key="tagsForSources"):
    filter_tags_source = st.text_area(
        "Source tags to filter on (comma-seperated)",
        value="",
        key="src_filter_tags"
    )
    config_data["source_config"]["filter_tags"] = [
        t.strip() for t in filter_tags_source.split(',') if t.strip()
    ]
  else:
    config_data["source_config"]["filter_tags"] = None

# --- Target Config ---
st.header("Target Configuration")
with st.container(border=True):
  st.subheader("Target Config Details")
  config_data["target_config"]["instance_type"] = st.text_input(
      "Target Instance Type",
      value=config_data["target_config"]["instance_type"],
      key="tgt_instance_type"
  )

  config_data["target_config"]["view_id"]["space"] = st.selectbox(
      label="Target View ID Space",
      options=get_spaces(),
      key="tgt_view_id_space"
  )

  if config_data["target_config"]["view_id"]["space"] != None:
    st.markdown("#### Target View ID")

    target_view_id = st.selectbox(
      label="Select the View",
      options=get_views(config_data["target_config"]["view_id"]["space"]),
      format_func=(lambda id: id.external_id),
      key="tgt_view_id"
    )

    if target_view_id is not None:
        config_data["target_config"]["view_id"]["external_id"]=target_view_id.external_id
        config_data["target_config"]["view_id"]["version"]=target_view_id.version

  config_data["target_config"]["instance_space"] = st.selectbox(
      label="Target Instance Space",
      options=get_spaces(),
      key="tgt_instance_space"
  )

  fields_to_pull_tgt = st.text_area(
      "Target Fields to Pull (comma-separated)",
      value=", ".join(config_data["target_config"]["fields_to_pull"]),
      key="tgt_fields_to_pull"
  )
  config_data["target_config"]["fields_to_pull"] = [
      f.strip() for f in fields_to_pull_tgt.split(',') if f.strip()
  ]

  fields_to_contextualize_tgt = st.text_area(
      "Target Fields to Contextualize (comma-separated)",
      value=", ".join(config_data["target_config"]["fields_to_contextualize"]),
      key="tgt_fields_to_contextualize"
  )
  
  config_data["target_config"]["fields_to_contextualize"] = [
      f.strip() for f in fields_to_contextualize_tgt.split(',') if f.strip()
  ]

  if st.checkbox("Add tag filters", value=False, key="tagsForTargets"):
    filter_tags_target = st.text_area(
        "Target tags to filter on (comma-seperated)",
        value="",
        key="tgt_filter_tags"
    )
    config_data["target_config"]["filter_tags"] = [
        t.strip() for t in filter_tags_target.split(',') if t.strip()
    ]
  else:
    config_data["target_config"]["filter_tags"] = None

# --- Contextualization Config ---
st.header("Contextualization Configuration")
with st.container(border=True):
  st.subheader("Contextualization Config Details")
  config_data["contextualization_config"]["num_matches"] = st.number_input(
      "Number of Matches",
      value=config_data["contextualization_config"]["num_matches"],
      min_value=0,
      step=1,
      key="ctx_num_matches"
  )

  config_data["contextualization_config"]["score_threshold"] = st.number_input(
      "Score Threshold",
      value=config_data["contextualization_config"]["score_threshold"],
      min_value=0.01,
      max_value=1.0,
      step=0.01,
      format="%.2f",
      key="ctx_score_threshold"
  )

  config_data["contextualization_config"]["write_true_matches"] = st.checkbox(
    "Write True Matches",
    value=config_data["contextualization_config"]["write_true_matches"],
    key="ctx_sup_write_true_matches"
  )


  config_data["contextualization_config"]["true_matches_table"]["database_name"] = st.selectbox(
    "True Matches Table Database Name",
    options=get_databases(),
    key="ctx_sup_true_db_name"
  )

  config_data["contextualization_config"]["true_matches_table"]["table_name"] = st.selectbox(
    "True Matches Table Name",
    options=get_tables(config_data["contextualization_config"]["true_matches_table"]["database_name"]),
    key="ctx_sup_true_table_name"
  )

  config_data["contextualization_config"]["true_match_threshold"] = st.number_input(
    "True Match Threshold",
    value=config_data["contextualization_config"]["true_match_threshold"],
    min_value=config_data["contextualization_config"]["score_threshold"],
    max_value=1.0,
    step=0.01,
    format="%.2f",
    key="ctx_sup_true_match_threshold"
  )

  if st.checkbox("Supervised?"):
    st.markdown("#### Supervised Config")


    if st.checkbox("Use existing model?"):
        config_data["contextualization_config"]["supervised_config"]["id"] = st.selectbox(
            "Supervised Config ID",
            options=get_models(),
            key="ctx_sup_id"
        )
    else:
        config_data["contextualization_config"]["supervised_config"]["id"]=None

  else:
    config_data["contextualization_config"]["supervised_config"]=None

  st.markdown("#### Contextualization Model Config")
  config_data["contextualization_config"]["contextualization_model_config"]["feature_type"] = st.selectbox(
      label="Feature Type",
      options=["Simple", "Insensitive", "Bigram", "FrequencyWeightedBigram", "BigramExtraTokenizers", "BigramCombo"],
      placeholder="Bigram",
      key="ctx_model_feature_type"
  )
  config_data["contextualization_config"]["contextualization_model_config"]["timeout"] = st.number_input(
      "Timeout (seconds)",
      value=config_data["contextualization_config"]["contextualization_model_config"]["timeout"],
      min_value=0,
      step=1,
      key="ctx_model_timeout"
  )

  st.markdown("#### Match Result Table")
  config_data["contextualization_config"]["match_result_table"]["database_name"] = st.selectbox(
      "Match Result Table Database Name",
      options=get_databases(),
      key="ctx_match_db_name"
  )
  config_data["contextualization_config"]["match_result_table"]["table_name"] = st.selectbox(
      "Match Result Table Name",
      options=get_tables(config_data["contextualization_config"]["match_result_table"]["database_name"]),
      key="ctx_match_table_name"
  )

# --- Log Level ---
st.header("Log Level")
log_levels = ["DEBUG", "INFO"]
config_data["log_level"] = st.selectbox(
    "Select Log Level",
    options=log_levels,
    index=log_levels.index(config_data["log_level"]),
    key="log_level"
)

interval=""
# --- Schedule ---
if st.checkbox("Run on schedule?"):
    interval_temp=st.text_input(label="CRON Expression - no more than every 10 minutes", value="*/10 * * * *") # <- will this work? idk
    if check_cron(interval_temp):
        interval=interval_temp
    else:
       st.error("Please enter a valid CRON expression...")

st.markdown("---")

if st.button("Preview"):
    
    with st.spinner("Working..."):
        time.sleep(1)
        
        st.write("Preview")
        st.write(config_data)

if st.button('Confirm'):
    new_state=get_new_state_df(config_data, interval)
    add_state(new_state)
    st.success(f"State Added with ID {new_state.index[0]}")