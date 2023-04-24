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
            'INSERT INTO one_to_one_topic_mapping (table_unique_key,one_to_one_id,one_to_one_token_id,'
            'vsoto_token_topic_pool_created_at,vsoto_token_topic_pool_created_by,'
            'vsoto_token_topic_created_at,vsoto_token_topic_created_by_id,topic_pool_id,topic_id)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set one_to_one_id = EXCLUDED.one_to_one_id,'
            'one_to_one_token_id = EXCLUDED.one_to_one_token_id,'
            'vsoto_token_topic_pool_created_at = EXCLUDED.vsoto_token_topic_pool_created_at,'
            'vsoto_token_topic_pool_created_by = EXCLUDED.vsoto_token_topic_pool_created_by,'
            'vsoto_token_topic_created_at = EXCLUDED.vsoto_token_topic_created_at,'
            'vsoto_token_topic_created_by_id = EXCLUDED.vsoto_token_topic_created_by_id,'
            'topic_pool_id = EXCLUDED.topic_pool_id,'
            'topic_id = EXCLUDED.topic_id;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
                transform_row[6],
                transform_row[7],
                transform_row[8]
            )
        )
    pg_conn.commit()


dag = DAG(
    'one_to_one_topic_mapping_dag',
    default_args=default_args,
    description='Maps one to one id with tokens topic pools and topics',
    schedule_interval='30 2 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS one_to_one_topic_mapping (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            one_to_one_id bigint,
            one_to_one_token_id bigint,
            vsoto_token_topic_pool_created_at timestamp,
            vsoto_token_topic_pool_created_by bigint,
            vsoto_token_topic_created_at timestamp,
            vsoto_token_topic_created_by_id bigint,
            topic_pool_id int,
            topic_id int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select distinct
    concat(video_sessions_onetoonetokentopicpoolmapping.one_to_one_token_id, row_number() over (order by video_sessions_onetoonetokentopicpoolmapping.one_to_one_token_id)) as table_unique_key,
    video_sessions_onetoone.id as one_to_one_id,
    video_sessions_onetoonetokentopicpoolmapping.one_to_one_token_id,
    video_sessions_onetoonetokentopicpoolmapping.created_at as vsoto_token_topic_pool_created_at,
    video_sessions_onetoonetokentopicpoolmapping.created_by_id as vsoto_token_topic_pool_created_by,
    video_sessions_onetoonetokentopicmapping.created_at as vsoto_token_topic_created_at,
    video_sessions_onetoonetokentopicmapping.created_by_id as vsoto_token_topic_created_by_id,
    video_sessions_onetoonetokentopicpoolmapping.topic_pool_id,
    video_sessions_onetoonetokentopicmapping.topic_id
from
    video_sessions_onetoonetokentopicpoolmapping
left join video_sessions_onetoonetokentopicmapping
    on video_sessions_onetoonetokentopicmapping.one_to_one_token_id = video_sessions_onetoonetokentopicpoolmapping.one_to_one_token_id
left join video_sessions_onetoone
    on video_sessions_onetoone.one_to_one_token_id = video_sessions_onetoonetokentopicpoolmapping.one_to_one_token_id
group by 2,3,4,5,6,7,8,9;
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