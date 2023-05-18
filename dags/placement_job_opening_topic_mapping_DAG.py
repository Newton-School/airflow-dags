from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}


def extract_data_to_nested(**kwargs):
    def clean_input(data_type, data_value):
        if data_type == 'string':
            return 'null' if not data_value else f'\"{data_value}\"'
        elif data_type == 'datetime':
            return 'null' if not data_value else f'CAST(\'{data_value}\' As TIMESTAMP)'
        else:
            return data_value

    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO placements_job_opening_topic_mapping (job_opening_topic_mapping_id,created_at,'
            'created_by_id,job_opening_id,topic_id,topic_title) '
            'VALUES (%s,%s,%s,%s,%s,%s)'
            'on conflict (job_opening_topic_mapping_id) do update set job_opening_id = EXCLUDED.job_opening_id, '
            'topic_id = EXCLUDED.topic_id, topic_title = EXCLUDED.topic_title;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5]
            )
        )
    pg_conn.commit()


dag = DAG(
    'placements_job_opening_topic_mapping_dag',
    default_args=default_args,
    description='Placements Job Opening Topic mapping DAG',
    schedule_interval='0 14 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS placements_job_opening_topic_mapping (
            job_opening_topic_mapping_id bigint not null PRIMARY KEY,
            created_at TIMESTAMP,
            created_by_id bigint,
            job_opening_id bigint,
            topic_id int,
            topic_title varchar(50)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            placements_jobopeningtopicmapping.id as job_opening_topic_mapping_id,
            placements_jobopeningtopicmapping.created_at,
            placements_jobopeningtopicmapping.created_by_id,
            placements_jobopeningtopicmapping.job_opening_id,
            placements_jobopeningtopicmapping.topic_id,
            technologies_topic.title as topic_title
            from placements_jobopeningtopicmapping
            left join technologies_topic on technologies_topic.id = placements_jobopeningtopicmapping.topic_id
    ;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
create_table >> transform_data >> extract_python_data