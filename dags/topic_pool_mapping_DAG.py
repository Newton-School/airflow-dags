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
                'INSERT INTO topic_pool_mapping (table_unique_key,topic_pool_id,'
                'topic_pool_created_at,created_by_id,difficulty_level,'
                'hash,one_to_one_type,parent_topic_pool_id,'
                'topic_pool_title,topic_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do nothing;',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                    transform_row[5],
                    transform_row[6],
                    transform_row[7],
                    transform_row[8],
                    transform_row[9],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'topic_pool_mapping_dag',
    default_args=default_args,
    description='A DAG for Topic Pool mapping',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS topic_pool_mapping (
            id Serial,
            table_unique_key not null bigint PRIMARY KEY,
            topic_pool_id int,
            topic_pool_created_at TIMESTAMP,
            created_by_id bigint,
            difficulty_level int,
            hash varchar(30),
            one_to_one_type int,
            parent_topic_pool_id int,
            topic_pool_title varchar(128),
            topic_id int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            distinct concat(technologies_topicpool.id,technologies_topicpoolmapping.topic_id) as table_unique_key,
            technologies_topicpool.id as topic_pool_id,
            technologies_topicpool.created_at as topic_pool_created_at,
            technologies_topicpool.created_by_id,
            technologies_topicpool.difficulty_level,
            technologies_topicpool.hash,
            technologies_topicpool.one_to_one_type,
            technologies_topicpool.parent_topic_pool_id,
            technologies_topicpool.title as topic_pool_title,
            technologies_topicpoolmapping.topic_id
            from technologies_topicpool
            left join technologies_topicpoolmapping on technologies_topicpool.id = technologies_topicpoolmapping.topic_pool_id;
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