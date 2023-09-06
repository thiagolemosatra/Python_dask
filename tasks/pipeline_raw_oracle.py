import dask.dataframe as dd
from dask.distributed import Client
import time
from sqlalchemy import table, column, select, types, Float
from sqlalchemy.dialects import oracle

#import asyncio
#import pandas as pd
#from pandera.typing.dask import DataFrame, Series
#import pandera as pa

client= Client("tcp://10.128.0.48:8786")
data="/tmp/daskdata/source/APDW_CARGA.txt"
url_db = 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'


def pipeline_raw_oracle(data):
    #le os dados
    df=dd.read_csv(data, sep="|", assume_missing=True).astype(dtype={
         'COD':'Int64',
         'COD_RA': 'Int64',
         'COD_MO': 'Int64',
         '00AAAJ': 'str',
         'DAT_ULT_AT': 'str',
         'QTD_DOC_SEG_N': 'Int64',
         'QTD_DOC_SEG_R': 'Int64',
         'VLR_DOC_SEG_EMIT': 'Float64',
         'VLR_PREMIO_COBRA': 'Float64',
         'VLR_SINISTRO_AVI': 'Float64',
         'VLR_SINISTRO_PAG': 'Float64'
                
    })
    df_result=(df.drop('Unnamed: 11', axis=1)
          .assign(DAT_ULT_AT=dd.to_datetime(df['DAT_ULT_AT'],format='%Y-%m-%d',errors='coerce'))
          .to_sql("tb_raw_data",url_db, if_exists='replace', index=False,  compute=True, parallel=True, chunksize=500,dtype={
             'COD':oracle.NUMBER,
             'COD_RA': oracle.NUMBER,
             'COD_MO': oracle.NUMBER,
             '00AAAJ': types.VARCHAR(6),
             'QTD_DOC_SEG_N':oracle.NUMBER,
             'QTD_DOC_SEG_R': oracle.NUMBER,
             'VLR_DOC_SEG_EMIT': oracle.FLOAT(binary_precision=53),
             'VLR_PREMIO_COBRA': oracle.FLOAT(binary_precision=53),
             'VLR_SINISTRO_AVI': oracle.FLOAT(binary_precision=53),
             'VLR_SINISTRO_PAG': oracle.FLOAT(binary_precision=53)
        }))
    print("Data frame lido")
start_time = time.time()
print(f"Iniciando Inserção")
pipeline_raw_oracle(data)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Inserção concluida,{elapsed_time} ")