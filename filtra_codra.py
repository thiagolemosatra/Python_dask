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


df = dd.read_sql_table(table_name='tb_raw_data', con=url_db,index_col= 'cod', meta= meta_schema)
df=client.persist(df)
def filtra_cod(cod):
    df_codra = client.persist(df[df['cod_ra']==cod])
    df_codra.to_sql(f"tb_codra_dask_{cod}",url_db, if_exists='replace', index=False, compute = True, parallel = True, chunksize=500,dtype={
            '00AAAJ': types.VARCHAR(6),
             'vlr_doc_seg_emit': oracle.FLOAT(binary_precision=53),
             'vlr_premio_cobra': oracle.FLOAT(binary_precision=53),
             'vlr_sinistro_avi': oracle.FLOAT(binary_precision=53),
             'vlr_sinistro_pag':oracle.FLOAT(binary_precision=53)
        })

async def escreve_tabelas():
    tasks = [asyncio.to_thread(filtra_cod, cod) for cod in df.cod_ra.unique()]
    res = await asyncio.gather(*tasks)

print("Iniciando inserção")
start_time = time.time()    
asyncio.get_event_loop().run_until_complete(escreve_tabelas())
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Inserção concluida,{elapsed_time} ")