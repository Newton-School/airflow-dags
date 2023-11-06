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
                'INSERT INTO mcq_topic_module_mapping (table_unique_key,'
                'mcq_id,'
                'topic_id,'
                'template_id,'
                'module_name) VALUES (%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set module_name = EXCLUDED.module_name;',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'mcq_topic_module_mapping_dag',
    default_args=default_args,
    description='Table mapping mcqs with topic_id and template_id',
    schedule_interval='45 19 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS mcq_topic_module_mapping (
            id serial, 
            table_unique_key text not null PRIMARY KEY,
            mcq_id bigint,
            topic_id bigint,
            template_id int,
            module_name text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        select 
            concat(assessments_multiplechoicequestion.id, '_', technologies_topic.id, '_', technologies_topictemplate.id) as table_unique_key,
            assessments_multiplechoicequestion.id as mcq_id,
            technologies_topic.id as topic_id,
            technologies_topictemplate.id as template_id,
            technologies_topictemplate.title as module_name
        from
            assessments_multiplechoicequestion
        join assessments_multiplechoicequestiontopicmapping
            on assessments_multiplechoicequestiontopicmapping.multiple_choice_question_id = assessments_multiplechoicequestion.id 
        join technologies_topic
            on technologies_topic.id = assessments_multiplechoicequestiontopicmapping.topic_id
        join technologies_topicnode
            on technologies_topicnode.topic_id = technologies_topic.id 
        join technologies_topictemplate
            on technologies_topictemplate.id = technologies_topicnode.topic_template_id
                and technologies_topictemplate.id in (208, 209, 367, 447, 489, 544, 555, 577, -- DS template_Ids
                102, 103, 119, 334, 336, 338, 339, 340, 341, 342, 344, 410 -- FSD template_Ids
                )
        group by 1,2,3,4,5;
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