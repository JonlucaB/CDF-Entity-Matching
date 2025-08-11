import streamlit as st
import pandas as pd
from data_processing import get_table_df
from ui import render_header, render_instructions, render_sidebar, render_main_content
import sys

def main():

  render_header()
  render_instructions()

  if "table_df" not in st.session_state:
    st.session_state.table_df=None
  if "selected_rows_df" not in st.session_state:
    st.session_state.selected_rows_df=pd.DataFrame()
  if "match_table_db_key" not in st.session_state:
    st.session_state.match_table_db_key=0
  if "match_table_tb_key" not in st.session_state:
    st.session_state.match_table_tb_key=sys.maxsize/4
  if "true_match_table_db_key" not in st.session_state:
    st.session_state.true_match_table_db_key=sys.maxsize/2
  if "true_match_table_tb_key" not in st.session_state:
    st.session_state.true_match_table_tb_key=int(sys.maxsize * 0.75)
  # if "score_threshold" not in st.session_state:
  #   st.session_state.score_threshold=1.0

  (db_name, table_name)=render_sidebar()

# --- Data Fetching and Processing ---
  # The main action of the app, triggered by the "Fetch Job Results" button.
  if db_name and table_name and st.sidebar.button("Fetch Job Results"):
      if db_name and table_name:
          # Show a spinner while data is being fetched and processed
          with st.spinner("Fetching and processing job results... Please wait."):
              st.session_state.table_df=get_table_df(db_name, table_name)

              if st.session_state.table_df is not None:
                  st.success("Job results fetched and processed successfully!")
                  # Initially, the filtered dataframe is the entire processed dataframe
                  st.session_state.selected_rows_df = pd.DataFrame()
              else:
                  st.error("Could not retrieve or process job results. Please check the Job ID.")
      else:
          st.warning("Please enter a Job ID to fetch results.")

    # --- Main Content Display ---
    # This block renders the main page content (charts, dataframes) only if
    # a job has been successfully loaded into the session state.
  if st.session_state.table_df is not None:
      render_main_content()

if __name__ == "__main__":
    main()