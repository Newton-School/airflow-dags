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
                'INSERT INTO placements_company_user_mapping (company_course_user_mapping_id,company_id,course_user_mapping_id,created_at,'
                'created_by_id,ctc,feedback,hash,job_opening_id,join_timestamp,placed_at,status,referred_at,latest_important_timestamp,'
                'job_opening_set_id,referral_accepted,reasons_for_rejection,job_opening_set,job_opening_set_created_at,'
                'job_opening_set_created_by_id,job_opening_set_opened,job_opening_set_opened_at,job_opening_set_opened_by_id,referral_set)'
                ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (company_course_user_mapping_id) do update set feedback =EXCLUDED.feedback,'
                'join_timestamp = EXCLUDED.join_timestamp,placed_at =EXCLUDED.placed_at,'
                'status=EXCLUDED.status,referred_at=EXCLUDED.referred_at,'
                'latest_important_timestamp=EXCLUDED.latest_important_timestamp,referral_accepted=EXCLUDED.referral_accepted,'
                'reasons_for_rejection=EXCLUDED.reasons_for_rejection,job_opening_set=EXCLUDED.job_opening_set,'
                'job_opening_set_created_at=EXCLUDED.job_opening_set_created_at,job_opening_set_created_by_id=EXCLUDED.job_opening_set_created_by_id,'
                'job_opening_set_opened=EXCLUDED.job_opening_set_opened,job_opening_set_opened_at=EXCLUDED.job_opening_set_opened_at,'
                'job_opening_set_opened_by_id=EXCLUDED.job_opening_set_opened_by_id,referral_set=EXCLUDED.referral_set,ctc=EXCLUDED.ctc,'
                'job_opening_id=EXCLUDED.job_opening_id,course_user_mapping_id=EXCLUDED.course_user_mapping_id ;',
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
                    transform_row[16],
                    transform_row[17],
                    transform_row[18],
                    transform_row[19],
                    transform_row[20],
                    transform_row[21],
                    transform_row[22],
                    transform_row[23],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'placements_company_user_mapping_dag',
    default_args=default_args,
    description='A DAG for referral user mapping',
    schedule_interval='35 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS placements_company_user_mapping (
            company_course_user_mapping_id bigint not null PRIMARY KEY,
            company_id bigint,
            course_user_mapping_id bigint,
            created_at TIMESTAMP,
            created_by_id bigint,
            ctc bigint,
            feedback varchar(900),
            hash varchar(15),
            job_opening_id int,
            join_timestamp TIMESTAMP,
            placed_at TIMESTAMP,
            status int,
            referred_at TIMESTAMP,
            latest_important_timestamp TIMESTAMP,
            job_opening_set_id  int,
            referral_accepted boolean,
            reasons_for_rejection int[],
            job_opening_set varchar(100),
            job_opening_set_created_at TIMESTAMP,
            job_opening_set_created_by_id bigint,
            job_opening_set_opened boolean,
            job_opening_set_opened_at TIMESTAMP,
            job_opening_set_opened_by_id bigint,
            referral_set varchar(15)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
                placements_companycourseusermapping.id as company_course_user_mapping_id,
                placements_companycourseusermapping.company_id,
                placements_companycourseusermapping.course_user_mapping_id,
                placements_companycourseusermapping.created_at,
                placements_companycourseusermapping.created_by_id,
                placements_companycourseusermapping.ctc,
                placements_companycourseusermapping.feedback,
                placements_companycourseusermapping.hash,
                placements_companycourseusermapping.job_opening_id,
                placements_companycourseusermapping.join_timestamp,
                placements_companycourseusermapping.placed_at,
                placements_companycourseusermapping.status,
                placements_companycourseusermapping.referred_at,
                placements_companycourseusermapping.latest_important_timestamp,
                placements_companycourseusermapping.job_opening_set_id,
                placements_companycourseusermapping.referral_accepted,
                placements_companycourseusermapping.reasons_for_rejection,
                placements_jobopeningset.title as job_opening_set,
                placements_jobopeningset.created_at as job_opening_set_created_at,
                placements_jobopeningset.created_by_id as job_opening_set_created_by_id,
                placements_jobopeningset.opened as job_opening_set_opened,
                placements_jobopeningset.opened_at as job_opening_set_opened_at,
                placements_jobopeningset.opened_by_id as job_opening_set_opened_by_id,
                case 
                when placements_jobopeningset.title like '%Automated%' then 'Automated'
                else 'Manual' end as referral_set
                from placements_companycourseusermapping
                left join placements_jobopeningset on placements_jobopeningset.id = placements_companycourseusermapping.job_opening_set_id
                order by 1
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