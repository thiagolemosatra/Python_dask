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

def consulta_tab_ref():
    tb=table('tb_raw_data',
        column('cod'),
        column('dat_ult_at'),
        column('vlr_doc_seg_emit'),
        column('vlr_premio_cobra'),
        column('vlr_sinistro_avi'),
        column('vlr_sinistro_pag')
    )
    df = dd.read_sql(sql=select(tb), con=url_db,index_col= 'cod', dtype={
            'vlr_doc_seg_emit': float,   
            'vlr_premio_cobra': float,
            'vlr_sinistro_avi': float,
            'vlr_sinistro_pag': float     
        
    })
    
    df_ref = dd.read_sql(sql='table_ref', con=url_db,index_col= 'cod')
    df_merge= dd.merge(df,df_ref, how='left', left_index = True, right_index = True)
    df_merge=(df_merge.fillna({
        'nome_cliente': "Cliente não encontrado"
    }).to_sql("tb_consulta_ref",url_db, if_exists='replace', index=True,  compute=True, parallel=True, chunksize=500,dtype={
                         'vlr_doc_seg_emit': oracle.FLOAT(binary_precision=53),
                         'vlr_premio_cobra': oracle.FLOAT(binary_precision=53),
                         'vlr_sinistro_avi': oracle.FLOAT(binary_precision=53),
                         'vlr_sinistro_pag': oracle.FLOAT(binary_precision=53)
                        }))

print("Iniciando inserção")
start_time = time.time()
consulta_tab_ref()
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Inserção concluida,{elapsed_time} ")
