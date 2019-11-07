from airflow import DAG
from datetime import datetime, timedelta
from sas import build_sas_dag

################################################################
# DAG PARAMETERS
################################################################

default_args = {
    'owner': 'CI-IT_SAS',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 21, 0, 0, 0),
    'email': ['user@example.com',
              ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),

}


dag_sas = DAG('sas_viernes',
              default_args=default_args,
              schedule_interval='0 10 * * 5',
              concurrency=1,
              max_active_runs=1,
)


build_sas_dag(dag_sas, '/sas/schedule/todeploy/semanal/viernes')
