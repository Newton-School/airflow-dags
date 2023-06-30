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
            'INSERT INTO arl_placements (table_unique_key,company_id,company_name,company_type,'
            'key_account_manager,sales_poc,job_opening_id,job_title,'
            'placement_role_title,number_of_rounds,number_of_openings,'
            'user_id,course_id,referral_set,referred_at,placed_at,'
            'round_type,round_start_date,round_end_date,round,no_show,'
            'round_status,company_status,company_status_prod,company_course_user_mapping_id,'
            'company_course_user_mapping_progress_id,round_new)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set company_name=EXCLUDED.company_name,'
            'company_type=EXCLUDED.company_type,key_account_manager=EXCLUDED.key_account_manager,'
            'sales_poc=EXCLUDED.sales_poc,job_title=EXCLUDED.job_title,'
            'placement_role_title=EXCLUDED.placement_role_title,number_of_rounds=EXCLUDED.number_of_rounds,'
            'number_of_openings=EXCLUDED.number_of_openings,referred_at=EXCLUDED.referred_at,'
            'placed_at=EXCLUDED.placed_at,round_type=EXCLUDED.round_type,'
            'round_start_date=EXCLUDED.round_start_date,round_end_date=EXCLUDED.round_end_date,'
            'round=EXCLUDED.round,no_show=EXCLUDED.no_show,round_status=EXCLUDED.round_status,'
            'company_status=EXCLUDED.company_status,company_status_prod=EXCLUDED.company_status_prod,'
            'company_course_user_mapping_progress_id=EXCLUDED.company_course_user_mapping_progress_id,'
            'round_new=EXCLUDED.round_new;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Placements',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Placements',
    schedule_interval='40 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_placements (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            company_id int,
            company_name varchar(100),
            company_type varchar(16),
            key_account_manager varchar(64),
            sales_poc varchar(64),
            job_opening_id bigint,
            job_title varchar(100),
            placement_role_title varchar(32),
            number_of_rounds int,
            number_of_openings int,
            user_id bigint,
            course_id int,
            referral_set varchar(16),
            referred_at DATE,
            placed_at DATE,
            round_type varchar(30),
            round_start_date DATE,
            round_end_date DATE,
            round varchar(16),
            no_show boolean,
            round_status varchar(16),
            company_status varchar(40),
            company_status_prod varchar(40),
            company_course_user_mapping_id bigint,
            company_course_user_mapping_progress_id bigint,
            round_new int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with a as(
        select
                    distinct concat(placements_company_user_mapping.company_course_user_mapping_id,placements_job_openings.job_opening_id,placements_round_progress.company_course_user_mapping_progress_id) as table_unique_key,
                    placements_company.company_id,
                    placements_company.company_name,
                    placements_company.company_type,
                    case when key_account_manager_1 is null then key_account_manager_2 else key_account_manager_1 end as key_account_manager,
                    case when sales_poc_1 is null then sales_poc_2 else sales_poc_1 end as sales_poc,
                    placements_job_openings.job_opening_id,
                    placements_job_openings.job_title,
                    placements_job_openings.placement_role_title,
                    placements_job_openings.number_of_rounds,
                    placements_job_openings.number_of_openings,
                    course_user_mapping.user_id,
                    course_user_mapping.course_id,
                    placements_company_user_mapping.referral_set,
                    date(placements_company_user_mapping.referred_at) as referred_at,
                    date(placements_company_user_mapping.placed_at) as placed_at,
                    case
                    when placements_round_progress.round_type = 1 then 'Resume'
                    when placements_round_progress.round_type = 2 then 'Tech Interview'
                    when placements_round_progress.round_type = 3 then 'HR'
                    when placements_round_progress.round_type = 4 then 'Assignment'
                    when placements_round_progress.round_type = 5 then 'Test' 
                    when placements_round_progress.round_type = 6 then 'Technical Telephonic Round' 
                    when placements_round_progress.round_type = 7 then 'HR Telephonic Round'
                    end as round_type,
                    date(placements_round_progress.start_timestamp) as round_start_date,
                    date(placements_round_progress.end_timestamp) as round_end_date,
                    case 
                    when placements_round_progress.round_1 = true then 'Round 1'
                    when placements_round_progress.round_2 = true then 'Round 2'
                    when placements_round_progress.round_3 = true then 'Round 3'
                    when placements_round_progress.round_4 = true then 'Round 4'
                    when placements_round_progress.pre_final_round = true then 'Pre-Final Round'
                    when placements_round_progress.final_round = true then 'Final Round' else null end as round,
                    placements_round_progress.no_show,
                    
                    case
                    when placements_round_progress.status = 1 then 'To be scheduled'
                    when placements_round_progress.status = 2 then 'Scheduled'
                    when placements_round_progress.status = 3 then 'Completed'
                    when placements_round_progress.status = 4 then 'Cleared'
                    when placements_round_progress.status = 5 then 'Not Cleared'
                    when placements_round_progress.status = 6 then 'Dropped'
                    when placements_round_progress.status = 7 then 'Re Scheduled'
                    when placements_round_progress.status = 8 then 'Not Completed' end as round_status,
                    
                    case 
                    when placements_company_user_mapping.status = 1 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 1 then 'In Process'
                    when placements_company_user_mapping.status = 2 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 2 then 'Company Rejected'
                    when placements_company_user_mapping.status = 3 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 3 then 'Candidate Selected'
                    when placements_company_user_mapping.status = 4 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 4 then 'Offer Letter Received'
                    when placements_company_user_mapping.status = 5 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 5 then 'Offer Letter Accepted'
                    when placements_company_user_mapping.status = 6 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 6 then 'Candidate Denied'
                    when placements_company_user_mapping.status = 7 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 7 then 'On Hold'
                    when placements_company_user_mapping.status = 8 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 8 then 'To be Shortlisted'
                    when placements_company_user_mapping.status = 9 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 9 then 'Shortlisted'
                    when placements_company_user_mapping.status = 10 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 10 then 'Shorlist Rejected'
                    when placements_company_user_mapping.status = 11 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 11 then 'Accepted another offer' 
                    when placements_company_user_mapping.status = 12 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 12 then 'Applied'
                    when placements_company_user_mapping.status = 13 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 13 then 'Application Rejected'
                    when placements_company_user_mapping.status = 14 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 14 then 'Application Shortlisted'
                    when placements_company_user_mapping.status = 15 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 15 then 'Candidate Withdrew'
                    when placements_company_user_mapping.status = 16 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 16 then 'Candidate Resigned'
                    when placements_company_user_mapping.status = 17 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 17 then 'Candidate Laid off'
                    when placements_company_user_mapping.status = 18 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 18 then 'Offer Confirmation Pending by Candidate'
                    when placements_company_user_mapping.status = 19 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 19 then 'Offer Confirmed by Candidate'
                    when placements_company_user_mapping.status = 20 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 20 then 'Candidate Joined Company'
                    when placements_company_user_mapping.status = 21 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 21 then 'Offer Cancelled by Company'
                    when placements_company_user_mapping.status = 22 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 22 then 'Process Stopped'
                    when placements_company_user_mapping.status = 23 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 23 then 'Company Shortlisted' 
                    when placements_company_user_mapping.status = 24 and placements_round_progress.status = 4 then 'Moved to next Round'
                    when placements_company_user_mapping.status = 24 then 'Placed in Another Company' 
                    end as company_status,
                    
                    case 
                    when placements_company_user_mapping.status = 1 then 'In Process'
                    when placements_company_user_mapping.status = 2 then 'Company Rejected'
                    when placements_company_user_mapping.status = 3 then 'Candidate Selected'
                    when placements_company_user_mapping.status = 4 then 'Offer Letter Received'
                    when placements_company_user_mapping.status = 5 then 'Offer Letter Accepted'
                    when placements_company_user_mapping.status = 6 then 'Candidate Denied'
                    when placements_company_user_mapping.status = 7 then 'On Hold'
                    when placements_company_user_mapping.status = 8 then 'To be Shortlisted'
                    when placements_company_user_mapping.status = 9 then 'Shortlisted'
                    when placements_company_user_mapping.status = 10 then 'Shorlist Rejected'
                    when placements_company_user_mapping.status = 11 then 'Accepted another offer' 
                    when placements_company_user_mapping.status = 12 then 'Applied'
                    when placements_company_user_mapping.status = 13 then 'Application Rejected'
                    when placements_company_user_mapping.status = 14 then 'Application Shortlisted'
                    when placements_company_user_mapping.status = 15 then 'Candidate Withdrew'
                    when placements_company_user_mapping.status = 16 then 'Candidate Resigned'
                    when placements_company_user_mapping.status = 17 then 'Candidate Laid off'
                    when placements_company_user_mapping.status = 18 then 'Offer Confirmation Pending by Candidate'
                    when placements_company_user_mapping.status = 19 then 'Offer Confirmed by Candidate'
                    when placements_company_user_mapping.status = 20 then 'Candidate Joined Company'
                    when placements_company_user_mapping.status = 21 then 'Offer Cancelled by Company'
                    when placements_company_user_mapping.status = 22 then 'Process Stopped'
                    when placements_company_user_mapping.status = 23 then 'Company Shortlisted' 
                    when placements_company_user_mapping.status = 24 then 'Placed in Another Company' 
                    end as company_status_prod,
                    
                    placements_company_user_mapping.company_course_user_mapping_id,
                    placements_round_progress.company_course_user_mapping_progress_id
                    
                    
                    from placements_company_user_mapping
                    left join placements_job_openings on placements_job_openings.job_opening_id = placements_company_user_mapping.job_opening_id
                    left join placements_company on placements_company.company_id = placements_job_openings.company_id
                    left join course_user_mapping on course_user_mapping.course_user_mapping_id = placements_company_user_mapping.course_user_mapping_id
                    left join placements_round_progress on placements_round_progress.company_course_user_mapping_id = placements_company_user_mapping.company_course_user_mapping_id
                    order by 2,placements_company_user_mapping.company_course_user_mapping_id,placements_round_progress.company_course_user_mapping_progress_id
        ),
        b as(
        select
                    distinct concat(placements_company_user_mapping.company_course_user_mapping_id,placements_job_openings.job_opening_id,'1') as table_unique_key,
                    placements_company.company_id,
                    placements_company.company_name,
                    placements_company.company_type,
                    case when key_account_manager_1 is null then key_account_manager_2 else key_account_manager_1 end as key_account_manager,
                    case when sales_poc_1 is null then sales_poc_2 else sales_poc_1 end as sales_poc,
                    placements_job_openings.job_opening_id,
                    placements_job_openings.job_title,
                    placements_job_openings.placement_role_title,
                    placements_job_openings.number_of_rounds,
                    placements_job_openings.number_of_openings,
                    course_user_mapping.user_id,
                    course_user_mapping.course_id,
                    placements_company_user_mapping.referral_set,
                    date(placements_company_user_mapping.referred_at) as referred_at,
                    date(placements_company_user_mapping.placed_at) as placed_at,
                    'Placed'  as round_type,
                    cast(null as date) as round_start_date,
                    cast(null as date) as round_end_date,
                    'Placed'  as round,
                    cast(null as boolean) as no_show,
                    
                   'Placed'  as round_status,
                    
                    'Placed' as company_status,
                    
                    case 
                    when placements_company_user_mapping.status = 1 then 'In Process'
                    when placements_company_user_mapping.status = 2 then 'Company Rejected'
                    when placements_company_user_mapping.status = 3 then 'Candidate Selected'
                    when placements_company_user_mapping.status = 4 then 'Offer Letter Received'
                    when placements_company_user_mapping.status = 5 then 'Offer Letter Accepted'
                    when placements_company_user_mapping.status = 6 then 'Candidate Denied'
                    when placements_company_user_mapping.status = 7 then 'On Hold'
                    when placements_company_user_mapping.status = 8 then 'To be Shortlisted'
                    when placements_company_user_mapping.status = 9 then 'Shortlisted'
                    when placements_company_user_mapping.status = 10 then 'Shorlist Rejected'
                    when placements_company_user_mapping.status = 11 then 'Accepted another offer' 
                    when placements_company_user_mapping.status = 12 then 'Applied'
                    when placements_company_user_mapping.status = 13 then 'Application Rejected'
                    when placements_company_user_mapping.status = 14 then 'Application Shortlisted'
                    when placements_company_user_mapping.status = 15 then 'Candidate Withdrew'
                    when placements_company_user_mapping.status = 16 then 'Candidate Resigned'
                    when placements_company_user_mapping.status = 17 then 'Candidate Laid off'
                    when placements_company_user_mapping.status = 18 then 'Offer Confirmation Pending by Candidate'
                    when placements_company_user_mapping.status = 19 then 'Offer Confirmed by Candidate'
                    when placements_company_user_mapping.status = 20 then 'Candidate Joined Company'
                    when placements_company_user_mapping.status = 21 then 'Offer Cancelled by Company'
                    when placements_company_user_mapping.status = 22 then 'Process Stopped'
                    when placements_company_user_mapping.status = 23 then 'Company Shortlisted' 
                    when placements_company_user_mapping.status = 24 then 'Placed in Another Company' 
                    end as company_status_prod,
                    
                    placements_company_user_mapping.company_course_user_mapping_id,
                    cast(null as int) as company_course_user_mapping_progress_id
                    
                    
                    
                    from placements_company_user_mapping
                    left join placements_job_openings on placements_job_openings.job_opening_id = placements_company_user_mapping.job_opening_id
                    left join placements_company on placements_company.company_id = placements_job_openings.company_id
                    left join course_user_mapping on course_user_mapping.course_user_mapping_id = placements_company_user_mapping.course_user_mapping_id
                    where placements_company_user_mapping.status in (3,4,6,11,18,21,5,16,17,19,20,25) and placed_at is not null
                    order by 2,placements_company_user_mapping.company_course_user_mapping_id
        ),
        c as(
        select
                    distinct concat(placements_company_user_mapping.company_course_user_mapping_id,placements_job_openings.job_opening_id,'0') as table_unique_key,
                    placements_company.company_id,
                    placements_company.company_name,
                    placements_company.company_type,
                    case when key_account_manager_1 is null then key_account_manager_2 else key_account_manager_1 end as key_account_manager,
                    case when sales_poc_1 is null then sales_poc_2 else sales_poc_1 end as sales_poc,
                    placements_job_openings.job_opening_id,
                    placements_job_openings.job_title,
                    placements_job_openings.placement_role_title,
                    placements_job_openings.number_of_rounds,
                    placements_job_openings.number_of_openings,
                    course_user_mapping.user_id,
                    course_user_mapping.course_id,
                    placements_company_user_mapping.referral_set,
                    date(placements_company_user_mapping.referred_at) as referred_at,
                    date(placements_company_user_mapping.placed_at) as placed_at,
                    'Referral'  as round_type,
                    cast(null as date) as round_start_date,
                    cast(null as date) as round_end_date,
                    'Referral'  as round,
                    cast(null as boolean) as no_show,
                    
                   'Referral'  as round_status,
                    
                    'Referral' as company_status,
                    
                    case 
                    when placements_company_user_mapping.status = 1 then 'In Process'
                    when placements_company_user_mapping.status = 2 then 'Company Rejected'
                    when placements_company_user_mapping.status = 3 then 'Candidate Selected'
                    when placements_company_user_mapping.status = 4 then 'Offer Letter Received'
                    when placements_company_user_mapping.status = 5 then 'Offer Letter Accepted'
                    when placements_company_user_mapping.status = 6 then 'Candidate Denied'
                    when placements_company_user_mapping.status = 7 then 'On Hold'
                    when placements_company_user_mapping.status = 8 then 'To be Shortlisted'
                    when placements_company_user_mapping.status = 9 then 'Shortlisted'
                    when placements_company_user_mapping.status = 10 then 'Shorlist Rejected'
                    when placements_company_user_mapping.status = 11 then 'Accepted another offer' 
                    when placements_company_user_mapping.status = 12 then 'Applied'
                    when placements_company_user_mapping.status = 13 then 'Application Rejected'
                    when placements_company_user_mapping.status = 14 then 'Application Shortlisted'
                    when placements_company_user_mapping.status = 15 then 'Candidate Withdrew'
                    when placements_company_user_mapping.status = 16 then 'Candidate Resigned'
                    when placements_company_user_mapping.status = 17 then 'Candidate Laid off'
                    when placements_company_user_mapping.status = 18 then 'Offer Confirmation Pending by Candidate'
                    when placements_company_user_mapping.status = 19 then 'Offer Confirmed by Candidate'
                    when placements_company_user_mapping.status = 20 then 'Candidate Joined Company'
                    when placements_company_user_mapping.status = 21 then 'Offer Cancelled by Company'
                    when placements_company_user_mapping.status = 22 then 'Process Stopped'
                    when placements_company_user_mapping.status = 23 then 'Company Shortlisted' 
                    when placements_company_user_mapping.status = 24 then 'Placed in Another Company' 
                    end as company_status_prod,
                    
                    placements_company_user_mapping.company_course_user_mapping_id,
                    cast(null as int) as company_course_user_mapping_progress_id
                    
                    
                    
                    from placements_company_user_mapping
                    left join placements_job_openings on placements_job_openings.job_opening_id = placements_company_user_mapping.job_opening_id
                    left join placements_company on placements_company.company_id = placements_job_openings.company_id
                    left join course_user_mapping on course_user_mapping.course_user_mapping_id = placements_company_user_mapping.course_user_mapping_id
                    where referred_at is not null
                    order by 2,placements_company_user_mapping.company_course_user_mapping_id
        
        ),
        d as(
        select * from a
        union all 
        select * from b
        union all 
        select * from c
        )
        select 
        table_unique_key,company_id,company_name,company_type,key_account_manager,sales_poc,job_opening_id,job_title,placement_role_title,number_of_rounds,number_of_openings,user_id,course_id,referral_set,referred_at,placed_at,round_type,round_start_date,round_end_date,round,no_show,round_status,company_status,company_status_prod,company_course_user_mapping_id,company_course_user_mapping_progress_id,
        case when round = 'Referral' then 1
        when round = 'Round 1' then 2
        when round = 'Round 2' then 3
        when round = 'Round 3' then 4
        when round = 'Round 4' then 5
        when round = 'Pre-Final Round' then 6
        when round = 'Final Round' then 7
        when round = 'Placed' then 8 end as round_new
        from d
        order by 
        case when round = 'Referral' then 1
        when round = 'Round 1' then 2
        when round = 'Round 2' then 3
        when round = 'Round 3' then 4
        when round = 'Round 4' then 5
        when round = 'Pre-Final Round' then 6
        when round = 'Final Round' then 7
        when round = 'Placed' then 8 end  
        ;
        ''',
    dag=dag
)

drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS arl_placements;
    ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
drop_table >> create_table >> transform_data >> extract_python_data