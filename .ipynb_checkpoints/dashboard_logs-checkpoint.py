import pandas as pd
#import plotly.express as px
import streamlit as st
import time

st.set_page_config(page_title = 'Dashboards Logs Dask',
                  layout = 'wide')

@st.experimental_memo
def get_data():
    return pd.read_sql_table('tb_deltas', 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE')

df = get_data()

placeholder = st.empty()

with placeholder.container():
    st.dataframe(df)
    time.sleep(1)
