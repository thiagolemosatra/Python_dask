import sys

# Adiciona um caminho personalizado ao sistema para importação de módulos
sys.path.insert(1, '/home/thiago_lemos/poc_dask_app')

import dask.dataframe as dd
import time
import parametros as par
import utils

from datetime import datetime 
from loguru import logger
from dask.distributed import Client

# Cria um cliente Dask distribuído para processamento paralelo
client = Client("tcp://10.128.0.48:8786")

# Instancia um objeto da classe 'ParamApdw' do módulo 'parametros'
p = par.ParamApdw()

# Obtém o caminho do arquivo de origem de 'p'
data = p.arquivo_origem

# Obtém a URL do banco de dados de 'p'
url_db = p.url_db

# Define a função para ler os dados brutos
def read_raw_data(data,id):
    try:
        # Registra uma mensagem informativa no log
        logger.info("Lendo dados")
        
        # Lê os dados do arquivo CSV usando Dask DataFrame
        df = dd.read_csv(data, sep="|", assume_missing=True).astype(dtype=p.raw_schema_base)
        
        # Registra uma mensagem de sucesso no log
        logger.success("Leitura concluída")
        
        return df
    except Exception as e:
        # Em caso de erro, registra uma mensagem de erro no log
        logger.error(f"Erro na leitura do arquivo: {e}")
        
        # Atualiza o log com informações de falha
        utils.update_log_fim(id=id, status='Falha', motivo_erro=f'{e.__class__.__name__}: {e}')
        
        # Encerra o programa
        sys.exit()

# Define a função para realizar o pipeline de dados brutos no Oracle
def pipeline_raw_oracle(df,id):
    try:
        start_time = time.time()
        
        # Registra uma mensagem informativa no log
        logger.info("Iniciando Inserção no Oracle")
        
        # Realiza o pipeline de dados, incluindo a conversão de tipos e inserção no Oracle
        df_result = (
            df.drop('Unnamed: 11', axis=1)
              .assign(DAT_ULT_AT=dd.to_datetime(df['DAT_ULT_AT'], format='%Y-%m-%d', errors='coerce'))
              .to_sql("tb_raw_data", url_db, if_exists='replace', index=False,  
                      compute=True, parallel=True, chunksize=500, dtype={
                          k: p.oracle_types[v] for (k, v) in p.schema_base.items() if k in (p.raw_schema_base.keys())
                      }
                     )            
        )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Registra uma mensagem de sucesso no log, incluindo o tempo de execução
        logger.success(f"Inserção concluída, {round(elapsed_time, 2)} segs ")
        
        # Atualiza o log com informações de sucesso
        utils.update_log_fim(id=id, status='Sucesso')
    except Exception as e:
        # Em caso de erro, registra uma mensagem de erro no log
        logger.error("Erro no envio bruto para o Oracle")
        
        # Atualiza o log com informações de falha
        utils.update_log_fim(id=id, status='Falha', motivo_erro=f'{e.__class__.__name__}:{e}')
        sys.exit(44)

# Obtém a data e hora atual
timestamp_ini = datetime.today()

# Registra o início do processo no log
id=utils.insere_log_inicio(nome_processo='Pipeline_Raw', timestamp_ini=timestamp_ini)

# Lê os dados brutos
df = read_raw_data(data, id)

# Realiza o pipeline de dados brutos no Oracle
pipeline_raw_oracle(df, id)
