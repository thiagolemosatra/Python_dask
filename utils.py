import sys
import dask.dataframe as dd
import time
import parametros as par
import sqlalchemy as sa

from loguru import logger
from dask.distributed import Client
from sqlalchemy import text, create_engine
from datetime import datetime 

p=par.ParamApdw()


#=========================================================================================================================
#Logs de deltas do Oracle


def insere_log_inicio(nome_processo,timestamp_ini):
    url_db= 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'
    engine= create_engine(url_db)
    with engine.connect() as conn:
        query= (text("insert into tb_deltas (nome_processo, status, horario_inicio_processo) values (:nome_processo,'Em execução', :hora_ini)")
                .bindparams(hora_ini=timestamp_ini,nome_processo=nome_processo))
        conn.execute(query)
        conn.commit()
def update_log_fim(nome_processo,timestamp_ini,status, motivo_erro= None):
    url_db= 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'
    engine= create_engine(url_db)
    timestamp_fim=datetime.today()
    with engine.connect() as conn:
        query= (text("update tb_deltas set status=:status, horario_fim_processo=:hora_fim, motivo_erro=:motivo_erro where horario_inicio_processo=:hora_ini and nome_processo=:nome_processo ")
            .bindparams(hora_fim=timestamp_fim,hora_ini=timestamp_ini,nome_processo=nome_processo, status=status, motivo_erro=motivo_erro))
        conn.execute(query)
        conn.commit()
#=========================================================================================================================

def read_oracle(url_db, timestamp_ini, colunas=None,meta=None):
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
       update_log_fim(nome_processo='read_oracle',timestamp_ini=timestamp_ini,status='Falha',motivo_erro=f'{e.__class__.__name__}: {e}')
       sys.exit(42)