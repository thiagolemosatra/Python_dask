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

def soma_valores_fevereiro():
    #define o select da tabela
    tb=table('tb_raw_data',
        column('cod'),
        column('dat_ult_at'),
        column('vlr_doc_seg_emit'),
        column('vlr_premio_cobra'),
        column('vlr_sinistro_avi'),
        column('vlr_sinistro_pag')
    )
    #le  a tabela
    df = dd.read_sql(sql=select(tb), con=url_db,index_col= 'cod', dtype={
        'dat_ult_at': 'datetime64[ns]',
        'vlr_doc_seg_emit': float,   
        'vlr_premio_cobra': float,
        'vlr_sinistro_avi': float,
        'vlr_sinistro_pag': float     
    }).reset_index()
    #Aplica as regras de negocio
    df=df.drop_duplicates()
    df_result = (df.assign(dat_ult_at=dd.to_datetime(df['dat_ult_at'],format='%Y-%m-%d',errors='coerce'))
                .query("cod==1 & dat_ult_at.between('2023-02-01','2023-02-28')")
                .groupby(['cod','dat_ult_at'])['vlr_doc_seg_emit','vlr_premio_cobra','vlr_sinistro_avi','vlr_sinistro_pag'].sum()
                .to_sql("tb_soma_valores_fevereiro",url_db, if_exists='append', index=True,  compute=True, parallel=True, chunksize=500, dtype={
                         'vlr_doc_seg_emit': oracle.FLOAT(binary_precision=53),
                         'vlr_premio_cobra': oracle.FLOAT(binary_precision=53),
                         'vlr_sinistro_avi': oracle.FLOAT(binary_precision=53),
                         'vlr_sinistro_pag': oracle.FLOAT(binary_precision=53)
                        }))

print("Iniciando inserção")
start_time = time.time()
soma_valores_fevereiro()
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Inserção concluida,{elapsed_time} ")