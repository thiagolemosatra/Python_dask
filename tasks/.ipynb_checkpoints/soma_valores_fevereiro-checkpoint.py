import sys
sys.path.insert(1,'/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd
import time
import parametros as par

from loguru import logger
from dask.distributed import Client
from utils import read_oracle as ro

client= Client("tcp://10.128.0.48:8786")
p=par.ParamApdw()
data=p.arquivo_origem
url_db = p.url_db

def soma_valores_fevereiro(df,colunas):
    #Aplica as regras de negocio
    try:
        start_time = time.time()
        logger.info("Iniciando Processo SomaValoresFevereiro")
        df=df.drop_duplicates()
        df_result = (df.assign(dat_ult_at=dd.to_datetime(df['dat_ult_at'],format='%Y-%m-%d',errors='coerce'))
                    .query("cod==1 & dat_ult_at.between('2023-02-01','2023-02-28')")
                    .groupby(['cod','dat_ult_at'])['vlr_doc_seg_emit','vlr_premio_cobra','vlr_sinistro_avi','vlr_sinistro_pag'].sum()
                    .to_sql("tb_soma_valores_fevereiro",url_db, if_exists='replace', index=True,  compute=True, parallel=True, chunksize=500, dtype={
                        k.lower(): p.oracle_types[v] for (k,v) in p.schema_base.items() if k.lower() in colunas
                    }
           ))
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.success(f"Inserção concluida,{round(elapsed_time,2)} segs ")
    except:
        logger.error(f"Erro na task SomaValoresFevereiro" )

colunas= ['cod','dat_ult_at','vlr_doc_seg_emit','vlr_premio_cobra','vlr_sinistro_avi','vlr_sinistro_pag']
df=ro(url_db,colunas)
soma_valores_fevereiro(df,colunas)

