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
            'INSERT INTO transcripts_data (id,prospect_id,created_at,sent_to_lead_squared,'
            'transcript_with_speaker_info,)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s);',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
                transform_row[6],
            )
        )
    pg_conn.commit()


dag = DAG(
    'transcripts_data_dag',
    default_args=default_args,
    description='A DAG for Transcripts Data from the Data Science Schema',
    schedule_interval='30 17 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS transcripts_data (
            id int,
            prospect_id varchar(512),
            created_at TIMESTAMP,
            sent_to_lead_squared boolean,
            transcript_with_speaker_info text[],
            speaker_00_count int,
            speaker_01_count int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_newton_ds',
    sql='''with rawest as(
            SELECT 
            name,
            SUBSTRING(name FROM 'Lead Squared Prospect ID - (.+)') AS prospect_id
            FROM open_ai_transcribeaudio
            WHERE name LIKE 'Lead Squared Prospect ID - %'
            ),
            raw as(
            SELECT
              id,
              prospect_id,
              created_at,
              sent_to_lead_squared,
              transcript_with_speaker_info,
              regexp_matches(transcript_with_speaker_info, E'\\w+', 'g') AS words_array,
              array_length(regexp_matches(transcript_with_speaker_info, E'\\w+', 'g'), 1) AS speaker_00_word_count
            FROM rawest
            left join open_ai_transcribeaudio on open_ai_transcribeaudio.name = rawest.name
            WHERE transcript_with_speaker_info ~* 'SPEAKER_00'
            ORDER BY id
            ),
            next_table as(
            select
            *,
            row_number() over (partition by id order by id) as rn
            from raw
            ),
            calc as(
            select
            id,
            prospect_id,
            created_at,
            sent_to_lead_squared,
            transcript_with_speaker_info,
            words_array,
            rn,
            lag(rn) over (partition by id),
            case 
            when lag(rn) over (partition by id) is null then rn 
            else rn - (lag(rn) over (partition by id)) end as words_count
            from next_table
            where words_array in ('{SPEAKER_01}','{SPEAKER_00}')
            )
            select
            distinct id,
            prospect_id,
            created_at,
            sent_to_lead_squared,
            transcript_with_speaker_info,
            sum(words_count) filter (where words_array = '{SPEAKER_00}') as speaker_00_count,
            sum(words_count) filter (where words_array = '{SPEAKER_01}') as speaker_01_count
            from calc
            group by 1,2,3,4,5
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