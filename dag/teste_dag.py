from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


dag= DAG('dag_teste', schedule_interval=timedelta(days=1),start_date=days_ago(1))

t1= BashOperator(task_id='raw_to_oracle',bash_command='conda run -n poc_dask python /home/thiago_lemos/poc_dask_app/pipeline_raw_oracle.py',dag=dag)
t2= BashOperator(task_id='gera_relatorio_fev',bash_command='conda run -n poc_dask python /home/thiago_lemos/poc_dask_app/soma_valores_fevereiro.py',dag=dag)
t3= BashOperator(task_id='enriquece_tab_origin',bash_command='conda run -n poc_dask python /home/thiago_lemos/poc_dask_app/consulta_tab_ref.py',dag=dag)
t4= BashOperator(task_id='gera_tab_codra',bash_command='conda run -n poc_dask python /home/thiago_lemos/poc_dask_app/filtra_codra.py',dag=dag)


t1 >> [t2,t3,t4]
