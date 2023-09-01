import dask.dataframe as dd
from dask.distributed import Client
import time
import asyncio
import pandas as pd
#from pandera.typing.dask import DataFrame, Series
#import pandera as pa
from sqlalchemy import table, column, select, types, Float
from sqlalchemy.dialects import oracle

client= Client("tcp://10.128.0.48:8786")
data="/tmp/daskdata/source/APDW_CARGA.txt"
url_db = 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'

meta_schema=pd.DataFrame({ 
 #'cod': pd.Series([],name='cod', dtype= 'Int64'),
 'cod_ra': pd.Series([],name='cod_ra', dtype= 'Int64'),
 'cod_mo': pd.Series([],name='cod_mo', dtype= 'Int64'),
 '00AAAJ': pd.Series([],name='00AAAJ', dtype= 'str'),
 'dat_ult_at': pd.Series([],name='dat_ult_at', dtype= 'datetime64[ns]'),
 'qtd_doc_seg_n': pd.Series([],name='qtd_doc_seg_n', dtype= 'Int64'),
 'qtd_doc_seg_r': pd.Series([],name='qtd_doc_seg_r', dtype= 'Int64'),
 'vlr_doc_seg_emit': pd.Series([],name='vlr_doc_seg_emit', dtype= 'Float64'),
 'vlr_premio_cobra': pd.Series([],name='vlr_premio_cobra', dtype= 'Float64'),
 'vlr_sinistro_avi': pd.Series([],name='vlr_sinistro_avi', dtype= 'Float64'),
 'vlr_sinistro_pag': pd.Series([],name='vlr_sinistro_pag', dtype= 'Float64')
 })

def pipeline_raw_oracle(data):
    #le os dados
    df=dd.read_csv(data, sep="|", assume_missing=True, dtype={'00AAAJ': str})
    df_result=(df.drop('Unnamed: 11', axis=1)
          .assign(DAT_ULT_AT=dd.to_datetime(df['DAT_ULT_AT'],format='%Y-%m-%d',errors='coerce'))
          .to_sql("tb_raw_data",url_db, if_exists='append', index=False,  compute=True, parallel=True, chunksize=500,dtype={
            '00AAAJ': types.VARCHAR(6),
             'vlr_doc_seg_emit': oracle.FLOAT(binary_precision=53),
             'vlr_premio_cobra': oracle.FLOAT(binary_precision=53),
             'vlr_sinistro_avi': oracle.FLOAT(binary_precision=53),
             'vlr_sinistro_pag': oracle.FLOAT(binary_precision=53)
        }))
    print("Data frame lido")

start_time = time.time()
pipeline_raw_oracle(data)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Inserção concluida,{elapsed_time} ")