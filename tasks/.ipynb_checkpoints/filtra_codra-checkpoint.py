import sys
sys.path.insert(1,'/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd

import time
import asyncio
import pandas as pd
import parametros as par
import utils
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

def filtra_cod(cod):
    try:
        start_time = time.time()
        logger.info(f"Iniciando inserção tabela {cod}")
        df_codra = client.persist(df[df['cod_ra']==cod])
        df_codra.to_sql(f"tb_codra_dask_{cod}",url_db, if_exists='replace', index=False, compute = True, parallel = True, chunksize=500,dtype={
            k.lower(): p.oracle_types[v] for (k,v) in p.schema_base.items()
            })
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.success(f"Inserção da tabela {cod} concluida,{round(elapsed_time,2)} segs ")
        

    except Exception as e:
        logger.error("Erro na task FiltraCodRa")
        utils.update_log_fim(nome_processo='Filtra_Cod_Ra',timestamp_ini=timestamp_ini,status='Falha',motivo_erro=f'{e.__class__.__name__}: {e}')
        sys.exit(42)
async def escreve_tabelas():
    tasks = [asyncio.to_thread(filtra_cod, cod) for cod in df.cod_ra.unique()]
    res = await asyncio.gather(*tasks)

timestamp_ini=datetime.today()
id=utils.insere_log_inicio(nome_processo='Filtra_Cod_Ra', timestamp_ini=timestamp_ini)
df=ro(url_db,id,meta=p.meta_schema)
df=client.persist(df)    
asyncio.get_event_loop().run_until_complete(escreve_tabelas())
utils.update_log_fim(id,status='Sucesso')

