from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime


default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
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
                'INSERT INTO weekly_user_details (course_user_mapping_id,user_id,'
                'admin_course_user_mapping_id,week_view,course_id,unit_type,'
                'status,label_mapping_id, course_name, course_structure_class, user_mapping_status)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'weekly_user_details_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='A DAG for maintaining WoW user data (user_id + course_id)',
    schedule_interval='30 19 * * MON',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS weekly_user_details (
            id serial not null PRIMARY KEY,
            course_user_mapping_id bigint not null,
            user_id bigint,
            admin_course_user_mapping_id bigint,
            week_view timestamp,
            course_id bigint,
            unit_type varchar(16),
            status int,
            label_mapping_id bigint,
            course_name varchar(255),
            course_structure_class varchar(255),
            user_mapping_status varchar(255)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
                courses_courseusermapping.id as course_user_mapping_id,
                courses_courseusermapping.user_id,
                courses_courseusermapping.admin_course_user_mapping_id,
                date_trunc('week', cast(now() as date)) as week_view,
                courses_course.id as course_id,
                courses_course.unit_type,
                courses_courseusermapping.status,
                courses_courseuserlabelmapping.id as label_mapping_id,
                courses_course.title as course_name,
                case 
                    when courses_course.course_structure_id in (1,18) then 'PAP'
                    when courses_course.course_structure_id in (8,22,23) then 'Upfront - MIA'
                    when courses_course.course_structure_id in (14,20,50,51,52,53,54,55,56,57,58,59,60) then 'Data Science - Certification'
                    when courses_course.course_structure_id in (11,26) then 'Data Science - IU'
                    when courses_course.course_structure_id in (6,12,19) then 'Upfront - FSD'
                end as course_structure_class,
                case
                    when courses_courseusermapping.status in (8,9) and courses_courseuserlabelmapping.id is null then 'Enrolled Student'
                    when courses_courseusermapping.status in (8,9) and courses_courseuserlabelmapping.id is not null then 'Label Marked User'
                    when courses_course.course_structure_id in (1,18) and courses_courseusermapping.status in (11,12) then 'ISA Cancelled User'
                    when courses_course.course_structure_id not in (1,18) and courses_courseusermapping.status in (11) then 'Foreclosed User'
                    when courses_course.course_structure_id not in (1,18) and courses_courseusermapping.status in (12) then 'Rejected By NS-Ops'
                    when courses_course.course_structure_id not in (1,18) and courses_courseusermapping.status in (30) then 'Deferred User'
                    else 'Other'
                end as user_mapping_status
            from
                courses_courseusermapping
            join courses_course 
                on courses_course.id = courses_courseusermapping.course_id and courses_course.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26,32,50,51,52,53,54,55,56,57,58,59,60)
            left join courses_courseuserlabelmapping 
                on courses_courseuserlabelmapping.course_user_mapping_id = courses_courseusermapping.admin_course_user_mapping_id and label_id = 677
            
            where courses_course.id in 
                
                (select distinct lu_course_id from
                
                    (with raw as  
                        (select 
                            *,
                            case
                                when (lu_csid in (1,18) and ((lu_start_date <= current_date) and (au_start_date + interval '15 Days') <= current_date) and au_end_date>= current_date and lu_end_date >= current_date) then 'Active'
                                when (lu_csid in (6,8,11,12,13,14,19,20,22,23,26,32,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121) and (au_start_date <= current_date) and au_end_date>= current_date and lu_end_date >= current_date) then 'Active'
                                else 'Inactive' end as batch_active_status_excluding_i2_check,
                            case 
                                when lu_course_id in (
                        (select distinct
                                courses_course.id as course_id
                            from
                                trainers_instructor
                            left join auth_user
                                on auth_user.id = trainers_instructor.user_id
                            left join trainers_courseinstructormapping
                                on trainers_courseinstructormapping.instructor_id = trainers_instructor.id
                            left join courses_course
                                on courses_course.id = trainers_courseinstructormapping.course_id
                            left join trainers_courseinstructorlabelmapping
                                on trainers_courseinstructorlabelmapping.course_instructor_mapping_id = trainers_courseinstructormapping.id
                            left join video_sessions_lecture
                                on video_sessions_lecture.course_id = courses_course.id
                            where trainers_instructor.is_active = true
                            and trainers_courseinstructormapping.is_active = true
                            and trainers_courseinstructorlabelmapping.id is not null
                            and lower(courses_course.title) not like '%archive%'
                            and lower(courses_course.title) not like '%simulation%'
                            and lower(courses_course.title) not like '%masters in computer science%'
                            and auth_user.email not like '%newtonschool%')) then 'Inst-2 Mapped' else 'Inst-2 Not Mapped' end as inst_2_status
                        
                        from
                        (select
                            cc.id as lu_course_id,
                            cc.title as lu_batch_name,
                            cc.start_timestamp as lu_start_date,
                            cc.end_timestamp as lu_end_date,
                            cc.course_structure_id as lu_csid,
                            courses_course.id as au_course_id,
                            courses_course.title as au_batch_name,
                            courses_course.start_timestamp as au_start_date,
                            courses_course.end_timestamp as au_end_date
                        from
                            courses_course as cc
                        join courses_courseusermapping ccum 
                            on ccum.course_id = cc.id
                        left join courses_courseusermapping
                            on ccum.admin_course_user_mapping_id = courses_courseusermapping.id and courses_courseusermapping.status not in (1,11,12,13,14,18,19,25,27)
                        left join courses_course 
                            on courses_course.id = courses_courseusermapping.course_id) a
                        where au_course_id is not null
                        -- and lu_end_date >= current_date
                        and lower(lu_batch_name) not like '%archive%'
                        -- and au_end_date >= current_date
                        group by 1,2,3,4,5,6,7,8,9,10,11)
                    select 
                        lu_course_id,
                        lu_batch_name,
                        lu_start_date,
                        lu_end_date,
                        case 
                            when lu_csid in (1,18) then 'PAP'
                            when lu_csid in (6,8,12,13,19,22,23) then 'Upfront - Non Data Science'
                            when lu_csid in (11,14,20,26,50,51,52,53,54,55,56,57,58,59,60) then 'Data Science + IU'
                            when lu_csid in (32) then 'Upfront - I2 batches'
                            when lu_csid in (127,118,119,122,121) then 'Advanced Software Development'
                            else 'No Tag' 
                        end as course_type,
                        au_course_id,
                        au_batch_name,
                        au_start_date,
                        au_end_date,
                        batch_active_status_excluding_i2_check,
                        inst_2_status,
                        case 
                            when batch_active_status_excluding_i2_check = 'Active' and inst_2_status = 'Inst-2 Not Mapped' then 'Active'
                            else 'Inactive'
                        end as final_batch_active_status
                    from
                        raw
                    where lower(lu_batch_name) not like '%simulation%'
                    and lower(au_batch_name) not like '%simulation%'
                    order by 1) a 
                where final_batch_active_status = 'Active');
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
