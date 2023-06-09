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
                'INSERT INTO wow_active_batches (lu_course_id,lu_batch_name,course_type,week_view,lu_start_date,'
                'lu_end_date,total_student_count,final_batch_active_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                    transform_row[5],
                    transform_row[6],
                    transform_row[7]
                 )
        )
    pg_conn.commit()


dag = DAG(
    'wow_active_batches_dag',
    default_args=default_args,
    description='This DAG maintains WoW list of all Active batches',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS wow_active_batches (
            id serial not null PRIMARY KEY,
            lu_course_id bigint not null,
            lu_batch_name varchar(100),
            course_type varchar(16),
            week_view timestamp,
            lu_start_date timestamp,
            lu_end_date timestamp,
            total_student_count int,
            final_batch_active_status varchar(16)
            
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select distinct
    lu_course_id,
    lu_batch_name,
    a.course_type,
    date_trunc('week', cast(now() as date)) as week_view,
    lu_start_date,
    lu_end_date,
    batch_strength.total_student_count,
    final_batch_active_status
from

    (with raw as  
        (select 
            *,
            case
                when (lu_csid in (1,18) and ((lu_start_date <= current_date) and (au_start_date + interval '15 Days') <= current_date) and au_end_date>= current_date and lu_end_date >= current_date) then 'Active'
                when (lu_csid in (6,8,11,12,13,14,19,20,22,23,26) and (au_start_date <= current_date) and au_end_date>= current_date and lu_end_date >= current_date) then 'Active'
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
            when lu_csid in (11,14,20,26) then 'Data Science + IU'
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
left join 

                    (with all_students as
                
                        (select
                            courses_course.title,
                            courses_courseuserlabelmapping.course_user_mapping_id,
                            courses_courseusermapping.user_id,
                            courses_courseuserlabelmapping.label_id 
                        from
                            courses_courseuserlabelmapping
                        left join courses_courseusermapping
                            on courses_courseusermapping.id = courses_courseuserlabelmapping.course_user_mapping_id and courses_courseuserlabelmapping.label_id = 677
                        left join courses_course
                            on courses_course.id = courses_courseusermapping.course_id
                        left join auth_user
                            on auth_user.id = courses_courseusermapping.user_id
                        where ((courses_course.course_structure_id in (6,8,11,12,13,14,19,20,22,23,26,24,32) and courses_courseusermapping.status in (8)) or (courses_course.course_structure_id in (1,18) and courses_courseusermapping.status in (5,9)))),
                        
                    student_raw as 
                        (select 
                            courses_course.id,
                            courses_course.title,
                            case 
                                when courses_course.course_structure_id in (1,18) then 'PAP'
                                else 'Upfront'
                            end as course_type,
                            courses_course.unit_type,
                            courses_course.start_timestamp,
                            courses_courseusermapping.user_id,
                            all_students.course_user_mapping_id,
                            courses_courseusermapping.status
                        from
                            courses_courseusermapping
                        left join all_students 
                            on all_students.course_user_mapping_id = courses_courseusermapping.admin_course_user_mapping_id
                        left join courses_course
                            on courses_course.id = courses_courseusermapping.course_id
                        left join auth_user
                            on auth_user.id = courses_courseusermapping.user_id
                        where ((courses_course.course_structure_id in (6,8,11,12,13,14,19,20,22,23,26,24,32) and courses_courseusermapping.status in (8)) or (courses_course.course_structure_id in (1,18) and courses_courseusermapping.status in (5,9)))
                        )
                        
                        
                    select
                        id,
                        title,
                        unit_type,
                        course_type,
                        date(start_timestamp) as course_start_date, 
                        -- count(distinct user_id) as total_students,
                        count(distinct user_id) filter (where student_raw.course_user_mapping_id is null) as total_student_count
                    from
                        student_raw
                    where lower(student_raw.title) not like '%archive%'
                    group by 1,2,3,4,5)  batch_strength
                        on batch_strength.id = a.lu_course_id
                    
where final_batch_active_status like 'Active'
and lu_course_id > 200;
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