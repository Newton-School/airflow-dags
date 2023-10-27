from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
from google.colab import auth
import gspread
from google.auth import default
from gspread_dataframe import set_with_dataframe
from airflow.models import Variable
  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

def update_ds_batch_intake_data(**kwargs):
  auth.authenticate_user()
  creds, _ = default()
  gc = gspread.authorize(creds)
  ds_batch_intake_metabase_username = Variable.get("DS_BATCH_INTAKE_METABASE_USERNAME", "")
  ds_batch_intake_metabase_password = Variable.get("DS_BATCH_INTAKE_METABASE_PASSWORD", "")
  res = requests.post('https://metabase.newtonschool.co/api/session',
                      headers = {"Content-Type": "application/json"},
                      json =  {"username": ds_batch_intake_metabase_username,
                              "password": ds_batch_intake_metabase_password}
                    )
  assert res.ok == True
  token = res.json()['id']
  res = requests.post('https://metabase.newtonschool.co/api/card/6031/query/json',
              headers = {'Content-Type': 'application/json',
                        'X-Metabase-Session': token
                        }
            )
  df = pd.DataFrame(res.json())
  df = df.fillna(value = 0)
  df = df.rename(columns = {'course_name':'Batch','module_name':'Module'})

  sheet = gc.open_by_key('1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A')
  worksheet = sheet.worksheet("Batch_Report")
  worksheet.clear()
  worksheet.update([df.columns.values.tolist()] + df.values.tolist())
  res = requests.post('https://metabase.newtonschool.co/api/card/6135/query/json',
              headers = {'Content-Type': 'application/json',
                        'X-Metabase-Session': token
                        }
            )
  df = pd.DataFrame(res.json())
  df = df.fillna(value = ' ')
  
  df['start_timestamp'] = pd.to_datetime(df['start_timestamp'])
  df['date'] = df['start_timestamp'].dt.date
  res = requests.post('https://metabase.newtonschool.co/api/card/6107/query/json',
                headers = {'Content-Type': 'application/json',
                          'X-Metabase-Session': token
                          }
              )
  df = pd.DataFrame(res.json())
  df = df.fillna(value = 0)
  df = df.rename(columns = {'date':'Date'})


  sheet = gc.open_by_key('1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A')
  worksheet = sheet.worksheet("Intake_Filter")
  # clearing worksheet
  worksheet.clear()
  set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)



dag = DAG(
    'ds_batches_intake',
    default_args=default_args,
    description='ds batch intake data which updates every 1hour',
    schedule_interval='0 */1 * * *',
    catchup=False
)

update_batch_data = PythonOperator(
    task_id='update_batch_data',
    python_callable=update_ds_batch_intake_data,
    provide_context=True,
    dag=dag
)

update_batch_data