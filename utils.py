import sys
import dask.dataframe as dd
import time
import parametros as par
import sqlalchemy as sa

from loguru import logger
from dask.distributed import Client
from sqlalchemy import text, create_engine, insert, table, column, String, Column, Sequence, Integer, DateTime
from sqlalchemy.orm import Session, DeclarativeBase, mapped_column, Mapped
from datetime import datetime 

p=par.ParamApdw()


#=========================================================================================================================
#Logs de deltas do Oracle

class Base(DeclarativeBase):
    pass

class TB_DELTAS(Base):
    __tablename__ = 'TB_DELTAS'
    id: Mapped[int] = mapped_column(primary_key = True)
    nome_processo = mapped_column(String(255))
    status = mapped_column(String(255))
    motivo_erro = mapped_column(String(255))
    horario_inicio_processo = mapped_column(DateTime())
    horario_fim_processo = mapped_column(DateTime())

def insere_log_inicio(nome_processo,timestamp_ini):
    url_db= 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'
    engine= create_engine(url_db)
    with Session(engine) as session:
        orm_insert = (TB_DELTAS(nome_processo = nome_processo, 
                                status='Em Execução', 
                                horario_inicio_processo = timestamp_ini))
        stmt = session.add(orm_insert)
        session.commit()
    #session.flush()
        #print(orm_insert.id)
        return orm_insert.id
        
def update_log_fim(id,status, motivo_erro= None):
    url_db= 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'
    engine= create_engine(url_db)
    timestamp_fim=datetime.today()
    with engine.connect() as conn:
        query= (text("update tb_deltas set status=:status, horario_fim_processo=:hora_fim, motivo_erro=:motivo_erro where id=:id ")
            .bindparams(hora_fim=timestamp_fim,id=id, status=status, motivo_erro=motivo_erro))
        conn.execute(query)
        conn.commit()
#=========================================================================================================================

def read_oracle(url_db, id, colunas=None,meta=None):
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
       update_log_fim(nome_processo='read_oracle',id=id,status='Falha',motivo_erro=f'{e.__class__.__name__}: {e}')
       sys.exit(42)