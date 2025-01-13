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

# Create a table for placements job openings
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        CREATE TABLE IF NOT EXISTS placements_job_openings (
            job_opening_id BIGINT NOT NULL PRIMARY KEY,
            company_id BIGINT,
            city_id INT,
            created_at TIMESTAMP,
            created_by_id BIGINT,
            job_description VARCHAR(6500),
            hash VARCHAR(64),
            interview_process VARCHAR(3500),
            max_ctc INT,
            min_ctc INT,
            number_of_openings INT,
            short_description VARCHAR(300),
            job_title VARCHAR(128),
            updated_at TIMESTAMP,
            number_of_rounds INT,
            closure_date DATE,
            placement_role_title VARCHAR(32),
            employment_type INT,
            allow_auto_referral BOOLEAN,
            
            -- Company criteria fields
            company_criteria_under_grad TEXT[],
            company_criteria_post_grad TEXT[],
            company_criteria_under_grad_field_of_study TEXT[],
            company_criteria_post_grad_field_of_study TEXT[],
            company_criteria_under_grad_percentage TEXT[],
            company_criteria_post_grad_percentage TEXT[],
            company_criteria_tenth_percent TEXT[],
            company_criteria_twelfth_percent TEXT[],
            company_criteria_gender TEXT[],
            company_criteria_overall_work_experience_match_type INT,
            company_criteria_overall_work_experience TEXT[],
            company_criteria_notice_period TEXT[],
            company_criteria_bond TEXT[],
            company_criteria_technical_work_experience TEXT[]
        );
    ''',
    dag=dag
)

alter_table_v1 = PostgresOperator(
    task_id='alter_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        DO $$
        BEGIN
            -- Check if the column already has a length of 255
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'placements_job_openings' 
                  AND column_name = 'placement_role_title'
                  AND character_maximum_length = 255
            ) THEN
                -- Alter the column to set character length to 255
                ALTER TABLE placements_job_openings 
                ALTER COLUMN placement_role_title TYPE VARCHAR(255);
            END IF;
        END $$;
    ''',
    dag=dag
)

alter_table_v2 = PostgresOperator(
    task_id='alter_table_v2',
    postgres_conn_id='postgres_result_db',
    sql='''
        DO $$
        BEGIN
            -- Check if the column job_description is not already of type TEXT
            IF EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'placements_job_openings' 
                  AND column_name = 'job_description'
                  AND data_type != 'text'
            ) THEN
                -- Alter the column to change its type to TEXT
                ALTER TABLE placements_job_openings 
                ALTER COLUMN job_description TYPE TEXT;
            END IF;
        END $$;
    ''',
    dag=dag
)

# Task to transform data from the database
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        SELECT
            placements_jobopening.id AS job_opening_id,
            placements_jobopening.company_id,
            placements_jobopening.city_id,
            placements_jobopening.created_at,
            placements_jobopening.created_by_id,
            placements_jobopening.description AS job_description,
            placements_jobopening.hash,
            placements_jobopening.interview_process,
            placements_jobopening.max_ctc,
            placements_jobopening.min_ctc,
            placements_jobopening.number_of_openings,
            placements_jobopening.short_description,
            placements_jobopening.title AS job_title,
            placements_jobopening.updated_at,
            placements_jobopening.number_of_rounds,
            placements_jobopening.closure_date,
            placements_placementrole.title AS placement_role_title,
            placements_jobopening.employment_type,
            placements_jobopening.allow_auto_referral,
            
            -- Company criteria fields
            placements_jobopeningcriteriaug.match_values AS company_criteria_under_grad,
            placements_jobopeningcriteriapg.match_values AS company_criteria_post_grad,
            placements_jobopeningcriteriaugfs.match_values AS company_criteria_under_grad_field_of_study,
            placements_jobopeningcriteriapgfs.match_values AS company_criteria_post_grad_field_of_study,
            placements_jobopeningcriteriaugpercent.match_values AS company_criteria_under_grad_percentage,
            placements_jobopeningcriteriapgpercent.match_values AS company_criteria_post_grad_percentage,
            placements_jobopeningcriteriatenthpercent.match_values AS company_criteria_tenth_percent,
            placements_jobopeningcriteriatwelfthpercent.match_values AS company_criteria_twelfth_percent,
            placements_jobopeningcriteriagender.match_values AS company_criteria_gender,
            placements_jobopeningcriteriaowe.match_type AS company_criteria_overall_work_experience_match_type,
            placements_jobopeningcriteriaowe.match_values AS company_criteria_overall_work_experience,
            placements_jobopeningcriterianoticeperiod.match_values AS company_criteria_notice_period,
            placements_jobopeningcriteriabond.match_values AS company_criteria_bond,
            placements_jobopeningcriteriatechnicalworkex.match_values AS company_criteria_technical_work_experience
        
        FROM 
            placements_jobopening
        LEFT JOIN 
            placements_placementrole 
            ON placements_placementrole.id = placements_jobopening.placement_role_id
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriaug 
            ON placements_jobopeningcriteriaug.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriaug.criteria_type = 1
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriapg 
            ON placements_jobopeningcriteriapg.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriapg.criteria_type = 2
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriaugfs 
            ON placements_jobopeningcriteriaugfs.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriaugfs.criteria_type = 3
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriapgfs 
            ON placements_jobopeningcriteriapgfs.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriapgfs.criteria_type = 4
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriaugpercent 
            ON placements_jobopeningcriteriaugpercent.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriaugpercent.criteria_type = 5
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriapgpercent 
            ON placements_jobopeningcriteriapgpercent.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriapgpercent.criteria_type = 6
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriatenthpercent 
            ON placements_jobopeningcriteriatenthpercent.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriatenthpercent.criteria_type = 7
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriatwelfthpercent 
            ON placements_jobopeningcriteriatwelfthpercent.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriatwelfthpercent.criteria_type = 8
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriagender 
            ON placements_jobopeningcriteriagender.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriagender.criteria_type = 9
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriaowe 
            ON placements_jobopeningcriteriaowe.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriaowe.criteria_type = 12
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriterianoticeperiod 
            ON placements_jobopeningcriterianoticeperiod.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriterianoticeperiod.criteria_type = 13
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriabond 
            ON placements_jobopeningcriteriabond.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriabond.criteria_type = 14
        LEFT JOIN 
            placements_jobopeningcriteria AS placements_jobopeningcriteriatechnicalworkex 
            ON placements_jobopeningcriteriatechnicalworkex.job_opening_id = placements_jobopening.id 
            AND placements_jobopeningcriteriatechnicalworkex.criteria_type = 16;
    ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
create_table >> alter_table_v1 >> alter_table_v2 >> transform_data >> extract_python_data