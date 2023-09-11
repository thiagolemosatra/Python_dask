import sys
sys.path.insert(1,'/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd

import time
import asyncio
import pandas as pd
import parametros as par
#from pandera.typing.dask import DataFrame, Series
#import pandera as pa

from sqlalchemy import table, column, select, types, Float
from sqlalchemy.dialects import oracle
from loguru import logger
from utils import read_oracle as ro
from dask.distributed import Client


client= Client("tcp://10.128.0.48:8786")
p=par.ParamApdw()
data=p.arquivo_origem
url_db = p.url_db

def consulta_tab_ref(df, colunas):
    try:
        start_time = time.time()
        logger.info("Iniciando Processo ConsultaTabRef")
        df_ref = dd.read_sql(sql='table_ref', con=url_db,index_col= 'cod')
        df_merge= dd.merge(df,df_ref, how='left', left_index = True, right_index = True)
        df_merge=(df_merge.fillna({
            'nome_cliente': "Cliente não encontrado"
        }).to_sql("tb_consulta_ref",url_db, if_exists='replace', index=True,  compute=True, parallel=True, chunksize=500,dtype={
                        k.lower(): p.oracle_types[v] for (k,v) in p.schema_base.items() if k.lower() in colunas
                    }))
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.success(f"Inserção concluida,{round(elapsed_time,2)} segs ")
    except: 
        logger.error(f"Erro na task ConsultaTabRef" )


colunas= ['cod','dat_ult_at','vlr_doc_seg_emit','vlr_premio_cobra','vlr_sinistro_avi','vlr_sinistro_pag']
df=ro(url_db,colunas)
consulta_tab_ref(df,colunas)