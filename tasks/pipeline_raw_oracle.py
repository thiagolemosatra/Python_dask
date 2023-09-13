import sys
sys.path.insert(1,'/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd
import time
import parametros as par

from loguru import logger
from dask.distributed import Client

#import asyncio
#import pandas as pd
#from pandera.typing.dask import DataFrame, Series
#import pandera as pa
#from sqlalchemy import table, column, select, types, Float
#from sqlalchemy.dialects import oracle
#from datetime import datetime
#import sqlalchemy


client= Client("tcp://10.128.0.48:8786")
p=par.ParamApdw()
data=p.arquivo_origem
url_db = p.url_db

def read_raw_data(data):
    #le os dados
    try:
        logger.info("Lendo dados")
        df=dd.read_csv(data, sep="|", assume_missing=True).astype(dtype=p.raw_schema_base)
        logger.success("Leitura concluida")
        return df
    except Exception as e:
        logger.error(f"Erro na leitura do arquivo:{e}")

def pipeline_raw_oracle(df):    
    try:
        start_time = time.time()
        logger.info("Iniciando Inserção no Oracle")
        df_result=(
            df.drop('Unnamed: 11', axis=1)
              .assign(DAT_ULT_AT=dd.to_datetime(df['DAT_ULT_AT'],format='%Y-%m-%d',errors='coerce'))
              .to_sql("tb_raw_data",url_db, if_exists='replace', index=False,  
                      compute=True, parallel=True, chunksize=500,dtype={
                          k: p.oracle_types[v] for (k,v) in p.schema_base.items() if k in (p.raw_schema_base.keys())
                      }
                     )            
        )
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.success(f"Inserção concluida, {round(elapsed_time,2)} segs ")
    except:
        logger.error("Erro no envio bruto para o Oracle")   

df=read_raw_data(data)
pipeline_raw_oracle(df)
