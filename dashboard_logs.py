import pandas as pd
import plotly.express as px
import streamlit as st
import time

st.set_page_config(page_title = 'Dashboards Logs Dask',
                  layout = 'wide')

st.title('Dashboard Logs Dask')

#@st.cache_data
def get_data():
    return pd.read_sql_table('tb_deltas', 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE')

#df = get_data()

placeholder = st.empty()

with placeholder.container():
    df = get_data()
    st.dataframe(df)
   
    fig_col, _x = st.columns(2)
    with fig_col:
        st.markdown('Sucesso x Erros x Em Execução')
        fig = px.histogram(data_frame = df, x ='status', color = 'nome_processo')
        st.write(fig)

#st.legacy_caching.clear_cache()
time.sleep(1)
st.experimental_rerun()
