import streamlit as st
import pandas as pd
import altair as alt
from st_aggrid import AgGrid, GridOptionsBuilder
from data_processing import get_database_names, get_table_names, write_table

def render_header():
    """Renders the page title and applies custom CSS."""
    
    st.title("Entity Matching Results Visualization and Migration 🔍")
    st.markdown("""
        <style>
            .big-font {
                font-size: 20px !important;
                font-weight: bold;
            }
            .medium-font {
                font-size: 16px !important;
            }
            .instruction-box {
                background-color: #f0f2f6;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 20px;
                border: 1px solid #e0e0e0;
            }
        </style>
    """, unsafe_allow_html=True)

def render_instructions():
    """Displays a box with instructions on how to use the application."""
    st.markdown("""
        <p>This tool allows you to select matches from the job results and add them to another table (typically a true matches table that is used to train your model).<p>
        <div class="instruction-box">
            <p class="big-font">How to use this tool:</p>
            <ol class="medium-font">
                <li>Click "Fetch RAW Results" to load and process the data.</li>
                <li>Use the slider to filter matches by their confidence score.</li>
                <li>Analyze the score distribution using the interactive charts.</li>
                <li>Review the statistics and the detailed data table below.</li>
                <li>Select all the rows you would like to migrate to your selected table.</li>
                <li>Select the table you'd like to add the rows to.</li>
                <li>Press the 'Submit new rows to RAW' button to add the rows to the table</li>
            </ol>
        </div>
    """, unsafe_allow_html=True)

def get_table_config(tb_key, db_key) -> (str, str):
    table_name_input=None
    database_name_input = st.selectbox(
        key=f"select_box_{db_key}",
        label="Select the database the RAW table is in:",
        options=get_database_names(),
        index=None,
        placeholder="Select Database Name"
    )

    if database_name_input:
        table_name_input = st.selectbox(
            key=f"select_box_{tb_key}",
            label="Select the raw table:",
            options=get_table_names(database_name_input),
            index=None,
            placeholder="Select Table Name"
        )

    return (database_name_input, table_name_input)

def render_sidebar() -> (str, str):
    """Renders the sidebar with input widgets for job configuration."""
    with st.sidebar:
        st.header("RAW Table")
        (database_name_input, table_name_input)=get_table_config(st.session_state.match_table_tb_key, st.session_state.match_table_db_key)


    
    return database_name_input, table_name_input


def get_grid(df: pd.DataFrame):
    if len(df) == 0:
        return None

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        groupable=True,
        value=True,
        enableRowGroup=True,
        filterable=True,
        aggFunc='sum'
    )

    gb.configure_pagination(enabled=True, paginationPageSize=20)
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_side_bar(filters_panel=True, columns_panel=True, defaultToolPanel='filters')

    grid_options = gb.build()

    return AgGrid(
        df,
        gridOptions=grid_options,
        height=800,
        width='200%',
        allow_unsafe_jscode=True
    )

def remove_row(df: pd.DataFrame, index) -> pd.DataFrame:
    return df.drop(index)

def add_row(df: pd.DataFrame, row) -> pd.DataFrame:
    return pd.concat([df, pd.DataFrame(row)])

def render_main_content():
    st.write("Matches")

    # Copy the cached tables to this function
    match_rows_df=st.session_state.table_df.copy()

    # Render the score histogram
    render_score_hist()

    # Make the AgGrids
    match_rows=get_grid(st.session_state.filtered_df_processed)

    num_selected=len(match_rows['selected_rows']) if match_rows['selected_rows'] is not None and not match_rows['selected_rows'].empty else 0
    st.write(f"{num_selected} Matches Selected...")

    (true_database_name_input, true_table_name_input)=get_table_config(st.session_state.true_match_table_db_key, st.session_state.true_match_table_tb_key)

    if num_selected > 0 and true_database_name_input and true_table_name_input and st.button("Submit new rows to RAW"):
        rows_to_write=match_rows['selected_rows'].copy()
        write_table(true_database_name_input, true_table_name_input, rows_to_write)

def render_score_hist():
    df_processed = st.session_state.table_df.copy()
    if df_processed is not None and not df_processed.empty:
        scores_df = df_processed[['score']].copy().dropna()
        
        if scores_df.empty:
            st.warning("No score data available to display.")
            return

        score_min = float(scores_df['score'].min())
        score_max = float(scores_df['score'].max())

        # Initialize or update the score threshold in session state
        st.session_state.score_threshold = st.slider(
            'Minimum Score Threshold',
            min_value=score_min,
            max_value=score_max,
            value=st.session_state.get('score_threshold', score_min),
            step=0.01
        )
        
        # Filter the dataframe based on the current threshold
        st.session_state.filtered_df_processed = df_processed[
            df_processed['score'] >= st.session_state.score_threshold
        ]

        # --- Altair Charts for Score Distribution ---
        col1, col2 = st.columns(2)
        threshold_rule = alt.Chart(pd.DataFrame({'score': [st.session_state.score_threshold]})).mark_rule(color='red', size=2).encode(x='score')

        with col1:
            st.subheader("Score Histogram")
            hist_chart = alt.Chart(scores_df).mark_bar().encode(
                alt.X('score:Q', bin=alt.Bin(maxbins=50), title='Score'),
                y=alt.Y('count():Q', title='Number of Matches')
            ).properties(height=300)
            st.altair_chart(hist_chart + threshold_rule, use_container_width=True)

        with col2:
            st.subheader("Score Cumulative Distribution (CDF)")
            cdf_chart = alt.Chart(scores_df).transform_window(
                cumulative_count='count(score)',
                sort=[{'field': 'score'}]
            ).transform_calculate(
                percentile='datum.cumulative_count / ' + str(len(scores_df))
            ).mark_line().encode(
                x=alt.X('score:Q', title='Score'),
                y=alt.Y('percentile:Q', axis=alt.Axis(format='%'), title='Cumulative Percentage')
            ).properties(height=300)
            st.altair_chart(cdf_chart + threshold_rule, use_container_width=True)
