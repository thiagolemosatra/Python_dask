import sys
sys.path.insert(1,'/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd
import utils
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
from datetime import datetime 


client= Client("tcp://10.128.0.48:8786")
p=par.ParamApdw()
data=p.arquivo_origem
url_db = p.url_db

def consulta_tab_ref(df, colunas,id):
    try:
        start_time = time.time()
        logger.info("Iniciando Processo Consulta_Tab_Ref")
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
        utils.update_log_fim(id,status='Sucesso')
    except Exception as e:
        logger.error(f"Erro na task ConsultaTabRef" )
        utils.update_log_fim(id,status='Falha',motivo_erro=f"{e.__class__.__name__}: {e}")
        sys.exit(43)

timestamp_ini=datetime.today()
id=utils.insere_log_inicio(nome_processo='Consulta_Tab_Ref',timestamp_ini=timestamp_ini)
colunas= ['cod','dat_ult_at','vlr_doc_seg_emit','vlr_premio_cobra','vlr_sinistro_avi','vlr_sinistro_pag']
df=ro(url_db,id,colunas)
consulta_tab_ref(df,colunas,id)