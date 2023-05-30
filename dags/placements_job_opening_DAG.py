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
            'INSERT INTO placements_job_openings (job_opening_id,company_id,city_id,created_at,'
            'created_by_id,job_description,hash,interview_process,max_ctc,min_ctc,number_of_openings,'
            'short_description,job_title,updated_at,number_of_rounds,closure_date,placement_role_title,'
            'employment_type,allow_auto_referral,company_criteria_under_grad,company_criteria_post_grad,'
            'company_criteria_under_grad_field_of_study,company_criteria_post_grad_field_of_study,'
            'company_criteria_under_grad_percentage,company_criteria_post_grad_percentage,'
            'company_criteria_tenth_percent,company_criteria_twelfth_percent,'
            'company_criteria_gender,company_criteria_overall_work_experience_match_type,'
            'company_criteria_overall_work_experience,company_criteria_notice_period,company_criteria_bond,'
            'company_criteria_technical_work_experience)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (job_opening_id) do update set updated_at = EXCLUDED.updated_at;',
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
                    transform_row[24],
                    transform_row[25],
                    transform_row[26],
                    transform_row[27],
                    transform_row[28],
                    transform_row[29],
                    transform_row[30],
                    transform_row[31],
                    transform_row[32],
                )
        )
        pg_conn.commit()


dag = DAG(
    'placements_job_openings_dag',
    default_args=default_args,
    description='A DAG for placements job opening details',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS placements_job_openings (
            job_opening_id bigint not null PRIMARY KEY,
            company_id bigint,
            city_id int,
            created_at TIMESTAMP,
            created_by_id bigint,
            job_description varchar(6500),
            hash varchar(64),
            interview_process varchar(3500),
            max_ctc int,
            min_ctc int,
            number_of_openings int,
            short_description varchar(300),
            job_title varchar(128),
            updated_at TIMESTAMP,
            number_of_rounds int,
            closure_date DATE,
            placement_role_title varchar(32),
            employment_type int,
            allow_auto_referral boolean,
            company_criteria_under_grad text[],
            company_criteria_post_grad text[],
            company_criteria_under_grad_field_of_study text[],
            company_criteria_post_grad_field_of_study text[],
            company_criteria_under_grad_percentage text[],
            company_criteria_post_grad_percentage text[],
            company_criteria_tenth_percent text[],
            company_criteria_twelfth_percent text[],
            company_criteria_gender text[],
            company_criteria_overall_work_experience_match_type int,
            company_criteria_overall_work_experience text[],
            company_criteria_notice_period text[],
            company_criteria_bond text[],
            company_criteria_technical_work_experience text[]
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            placements_jobopening.id as job_opening_id,
            placements_jobopening.company_id,
            placements_jobopening.city_id,
            placements_jobopening.created_at,
            placements_jobopening.created_by_id,
            placements_jobopening.description as job_description,
            placements_jobopening.hash,
            placements_jobopening.interview_process,
            placements_jobopening.max_ctc,
            placements_jobopening.min_ctc,
            placements_jobopening.number_of_openings,
            placements_jobopening.short_description,
            placements_jobopening.title as job_title,
            placements_jobopening.updated_at,
            placements_jobopening.number_of_rounds,
            placements_jobopening.closure_date,
            placements_placementrole.title as placement_role_title,
            placements_jobopening.employment_type,
            placements_jobopening.allow_auto_referral,
            placements_jobopeningcriteriaug.match_values as company_criteria_under_grad,
            placements_jobopeningcriteriapg.match_values as company_criteria_post_grad,
            placements_jobopeningcriteriaugfs.match_values as company_criteria_under_grad_field_of_study,
            placements_jobopeningcriteriapgfs.match_values as company_criteria_post_grad_field_of_study,
            placements_jobopeningcriteriaugpercent.match_values as company_criteria_under_grad_percentage,
            placements_jobopeningcriteriapgpercent.match_values as company_criteria_post_grad_percentage,
            placements_jobopeningcriteriatenthpercent.match_values as company_criteria_tenth_percent,
            placements_jobopeningcriteriatwelfthpercent.match_values as company_criteria_twelfth_percent,
            placements_jobopeningcriteriagender.match_values as company_criteria_gender,
            placements_jobopeningcriteriaowe.match_type as company_criteria_overall_work_experience_match_type,
            placements_jobopeningcriteriaowe.match_values as company_criteria_overall_work_experience,
            placements_jobopeningcriterianoticeperiod.match_values as company_criteria_notice_period,
            placements_jobopeningcriteriabond.match_values as company_criteria_bond,
            placements_jobopeningcriteriatechnicalworkex.match_values as company_criteria_technical_work_experience
            
            from placements_jobopening
            left join placements_placementrole on placements_placementrole.id = placements_jobopening.placement_role_id
            left join placements_jobopeningcriteria as placements_jobopeningcriteriaug on placements_jobopeningcriteriaug.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriaug.criteria_type = 1
            left join placements_jobopeningcriteria as placements_jobopeningcriteriapg on placements_jobopeningcriteriapg.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriapg.criteria_type = 2
            left join placements_jobopeningcriteria as placements_jobopeningcriteriaugfs on placements_jobopeningcriteriaugfs.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriaugfs.criteria_type = 3
            left join placements_jobopeningcriteria as placements_jobopeningcriteriapgfs on placements_jobopeningcriteriapgfs.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriapgfs.criteria_type = 4
            left join placements_jobopeningcriteria as placements_jobopeningcriteriaugpercent on placements_jobopeningcriteriaugpercent.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriaugpercent.criteria_type = 5
            left join placements_jobopeningcriteria as placements_jobopeningcriteriapgpercent on placements_jobopeningcriteriapgpercent.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriapgpercent.criteria_type = 6
            left join placements_jobopeningcriteria as placements_jobopeningcriteriatenthpercent on placements_jobopeningcriteriatenthpercent.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriatenthpercent.criteria_type = 7
            left join placements_jobopeningcriteria as placements_jobopeningcriteriatwelfthpercent on placements_jobopeningcriteriatwelfthpercent.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriatwelfthpercent.criteria_type = 8
            left join placements_jobopeningcriteria as placements_jobopeningcriteriagender on placements_jobopeningcriteriagender.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriagender.criteria_type = 9
            left join placements_jobopeningcriteria as placements_jobopeningcriteriaowe on placements_jobopeningcriteriaowe.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriaowe.criteria_type = 12
            left join placements_jobopeningcriteria as placements_jobopeningcriterianoticeperiod on placements_jobopeningcriterianoticeperiod.job_opening_id = placements_jobopening.id and placements_jobopeningcriterianoticeperiod.criteria_type = 13
            left join placements_jobopeningcriteria as placements_jobopeningcriteriabond on placements_jobopeningcriteriabond.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriabond.criteria_type = 14
            left join placements_jobopeningcriteria as placements_jobopeningcriteriatechnicalworkex on placements_jobopeningcriteriatechnicalworkex.job_opening_id = placements_jobopening.id and placements_jobopeningcriteriatechnicalworkex.criteria_type = 16;
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