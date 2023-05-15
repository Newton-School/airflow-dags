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

            'INSERT INTO mcq_topic_and_label_mapping (table_unique_key,mcq_id,mcq_created_at,'
            'correct_choice,difficulty_level,hash,is_deleted,question_text,question_type,'
            'question_for_assessment_type,peer_reviewed,user_generated,topic_id,'
            'mcq_utility_type,mcq_relevance,label_id,label_name)'

            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            'on conflict (table_unique_key) do update set mcq_id = EXCLUDED.mcq_id,'
            'mcq_created_at = EXCLUDED.mcq_created_at,'
            'correct_choice = EXCLUDED.correct_choice,'
            'difficulty_level = EXCLUDED.difficulty_level,'
            'hash = EXCLUDED.hash,'
            'is_deleted = EXCLUDED.is_deleted,'
            'question_text = EXCLUDED.question_text,'
            'question_type = EXCLUDED.question_type,'
            'question_for_assessment_type = EXCLUDED.question_for_assessment_type,'
            'peer_reviewed = EXCLUDED.peer_reviewed,'
            'user_generated = EXCLUDED.user_generated,'
            'topic_id = EXCLUDED.topic_id,'
            'mcq_utility_type = EXCLUDED.mcq_utility_type,'
            'mcq_relevance = EXCLUDED.mcq_relevance,'
            'label_id = EXCLUDED.label_id,'
            'label_name = EXCLUDED.label_name;',
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
                transform_row[10],
                transform_row[11],
                transform_row[12],
                transform_row[13],
                transform_row[14],
                transform_row[15],
                transform_row[16]
            )
        )
    pg_conn.commit()


dag = DAG(
    'mcq_topic_and_label_mapping_dag',
    default_args=default_args,
    description='Multiple choice questions (MCQs) mapping with topics and labels',
    schedule_interval='0 17 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS mcq_topic_and_label_mapping (
            id serial not null,
            table_unique_key bigint not null PRIMARY KEY,
            mcq_id bigint not null,
            mcq_created_at timestamp,
            correct_choice int,
            difficulty_level int,
            hash varchar(32),
            is_deleted boolean,
            question_text varchar(8256),
            question_type int,
            question_for_assessment_type integer[],
            peer_reviewed boolean,
            user_generated boolean,
            topic_id bigint,
            mcq_utility_type int,
            mcq_relevance int,
            label_id int,
            label_name varchar(256)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    cast(concat(assessments_multiplechoicequestion.id, assessments_multiplechoicequestiontopicmapping.topic_id, technologies_label.id) as bigint) as table_unique_key,
    assessments_multiplechoicequestion.id as mcq_id,
    assessments_multiplechoicequestion.created_at as mcq_created_at,
    assessments_multiplechoicequestion.correct_choice,
    assessments_multiplechoicequestion.difficulty_level,
    assessments_multiplechoicequestion.hash,
    assessments_multiplechoicequestion.is_deleted,
    assessments_multiplechoicequestion.question_text,
    assessments_multiplechoicequestion.question_type,
    assessments_multiplechoicequestion.question_for_assessment_type,
    assessments_multiplechoicequestion.peer_reviewed,
    assessments_multiplechoicequestion.user_generated,
    assessments_multiplechoicequestiontopicmapping.topic_id,
    assessments_multiplechoicequestiontopicmapping.question_utility_type as mcq_utility_type,
    assessments_multiplechoicequestiontopicmapping.relevance as mcq_relevance,
    technologies_label.id as label_id,
    technologies_label.name as label_name
    
from
    assessments_multiplechoicequestion
left join assessments_multiplechoicequestiontopicmapping
    on assessments_multiplechoicequestiontopicmapping.multiple_choice_question_id = assessments_multiplechoicequestion.id
left join assessments_multiplechoicequestionlabelmapping
    on assessments_multiplechoicequestionlabelmapping.multiple_choice_question_id = assessments_multiplechoicequestion.id
left join technologies_label
    on technologies_label.id = assessments_multiplechoicequestionlabelmapping.label_id
order by 2;
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