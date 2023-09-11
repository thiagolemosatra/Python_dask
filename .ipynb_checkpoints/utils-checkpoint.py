import sys
import dask.dataframe as dd
import time
import parametros as par
import sqlalchemy
from loguru import logger
from dask.distributed import Client



p=par.ParamApdw()

def read_oracle(url_db, colunas=None,meta=None):
    #le  a tabela
   try:
        logger.info("Iniciando leitura do Oracle")
        if (colunas is None) & (meta is None) :
            df = dd.read_sql_table(table_name='tb_raw_data', con=url_db,index_col= 'cod', dtype={
                k.lower(): p.schema_base[k] for k in p.schema_base 
                }).reset_index()
            #erro abaixo sucess
            logger.success("Leitura de toda a tabela concluida.")
            return df
        elif meta is not None:
            df = dd.read_sql_table(table_name='tb_raw_data', con=url_db,index_col= 'cod', meta= p.meta_schema)
            logger.success("Leitura dos Metadados concluida.")
            return df
        else:
            df = dd.read_sql_table(table_name='tb_raw_data', columns=colunas, con=url_db,index_col= 'cod', dtype={
                k.lower(): p.schema_base[k] for k in p.schema_base if k.lower() in colunas
                }).reset_index()
            logger.success("Leitura das colunas concluida.")
            return df
   except Exception as e:
       logger.error(f"Erro na leitura do Oracle: {e.__class__.__name__}: {e}")
       sys.exit(42)
       