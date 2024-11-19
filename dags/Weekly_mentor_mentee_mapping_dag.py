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
            'INSERT INTO mentor_mentee_mapping (mentor_user_id,'
            'course_id,batch_name,week_view,mentee_user_id)'
            'VALUES (%s,%s,%s,%s,%s);',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4]
            )
        )
    pg_conn.commit()


dag = DAG(
    'mentor_mentee_mapping_dag',
    default_args=default_args,
    description='Weekly dump of mentor and mapped mentees',
    schedule_interval='30 20 * * FRI',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS mentor_mentee_mapping (
            id serial not null PRIMARY KEY,
            mentor_user_id bigint,
            course_id bigint,
            batch_name varchar(256),
            week_view date,
            mentee_user_id bigint
    );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    trainers_mentor.user_id as mentor_user_id,
    courses_course.id as course_id,
    courses_course.title as batch_name,
    date(date_trunc('week', current_date)) as week_view,
    courses_courseusermapping.user_id as mentee_user_id
    
from
    trainers_mentor
left join auth_user
    on auth_user.id = trainers_mentor.user_id and trainers_mentor.is_active = true
left join trainers_coursementormapping
    on trainers_coursementormapping.mentor_id = trainers_mentor.id and trainers_coursementormapping.is_active = true
left join trainers_coursementorsubbatchmapping
    on trainers_coursementorsubbatchmapping.course_mentor_mapping_id = trainers_coursementormapping.id and trainers_coursementorsubbatchmapping.is_deleted = false
left join courses_subbatch
    on courses_subbatch.id = trainers_coursementorsubbatchmapping.sub_batch_id and courses_subbatch.is_deleted = false
left join courses_subbatchcourseusermapping
    on courses_subbatchcourseusermapping.sub_batch_id = courses_subbatch.id and courses_subbatchcourseusermapping.is_deleted = false
left join courses_course
    on courses_course.id = courses_subbatch.course_id and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,32,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121,94,95,131,132)
left join courses_courseusermapping
    on courses_courseusermapping.course_id = courses_course.id and courses_courseusermapping.id = courses_subbatchcourseusermapping.course_user_mapping_id and courses_courseusermapping.status in (5,8,9)
left join auth_user au2
    on au2.id = courses_courseusermapping.user_id
    
where courses_courseusermapping.id not in 
      
                    (
                        with student_raw as
                            
                            (select
                                    distinct courses_courseusermapping.user_id,
                                    cc.id as lu_course_id,
                                    cc.title as lu_batch_name,
                                    ccum.id as lu_cum_id,
                                    ccum.status as lu_cum_status,
                                    courses_courseuserlabelmapping.id as label_mapping_id,
                                    courses_course.id as au_course_id,
                                    courses_course.title as au_batch_name,
                                    courses_courseusermapping.id as au_cum_id,
                                    courses_courseusermapping.status as au_cum_status,
                                    courses_course.start_timestamp as AU_start_date
                                from 
                                    courses_courseusermapping
                                left join courses_courseuserlabelmapping
                                    on courses_courseuserlabelmapping.course_user_mapping_id = courses_courseusermapping.id and courses_courseuserlabelmapping.label_id = 677
                                left join courses_course 
                                    on courses_course.id = courses_courseusermapping.course_id
                                join courses_courseusermapping ccum 
                                    on ccum.admin_course_user_mapping_id = courses_courseusermapping.id
                                left join courses_course as cc 
                                    on ccum.course_id = cc.id
                                where ((courses_course.course_structure_id in (6,8,11,12,13,14,19,20,22,23,26,50,51,52,53,54,55,56,57,58,59,60) and courses_courseusermapping.status in (8)) or (courses_course.course_structure_id in (1,18) and courses_courseusermapping.status in (5,9)))
                                order by 1)
                                
                            select distinct
                                lu_cum_id    
                            from 
                                student_raw
                            where label_mapping_id is not null);
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