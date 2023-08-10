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
                'INSERT INTO course_user_mapping (course_user_mapping_id,user_id,course_id,'
                'course_name,unit_type,admin_course_user_mapping_id,admin_unit_name,'
                'admin_course_id,created_at,status,label_id,utm_campaign,utm_source,'
                'utm_medium,hash,apply_form_current_city,apply_form_graduation_year,apply_form_current_occupation,'
                'apply_form_work_ex, user_placement_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (course_user_mapping_id) do update set course_name=EXCLUDED.course_name,'
                'unit_type=EXCLUDED.unit_type,admin_unit_name=EXCLUDED.admin_unit_name,'
                'admin_course_id=EXCLUDED.admin_course_id,'
                'status = EXCLUDED.status,label_id = EXCLUDED.label_id,'
                'admin_course_user_mapping_id = EXCLUDED.admin_course_user_mapping_id,'
                'apply_form_current_city=EXCLUDED.apply_form_current_city,'
                'apply_form_graduation_year=EXCLUDED.apply_form_graduation_year,'
                'apply_form_current_occupation=EXCLUDED.apply_form_current_occupation,'
                'apply_form_work_ex=EXCLUDED.apply_form_work_ex,'
                'user_placement_status = EXCLUDED.user_placement_status;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'courses_cum_dag',
    default_args=default_args,
    description='Course user mapping detailed version',
    schedule_interval='0 21 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_user_mapping (
            course_user_mapping_id bigint not null PRIMARY KEY,
            user_id bigint,
            course_id bigint,
            course_name varchar(128),
            unit_type varchar(16),
            admin_course_user_mapping_id bigint,
            admin_unit_name varchar(128),
            admin_course_id bigint,
            created_at timestamp,
            status int,
            label_id bigint,
            utm_campaign varchar(256),
            utm_source varchar(256),
            utm_medium varchar(256),
            hash varchar(100),
            apply_form_current_city varchar(256),
            apply_form_graduation_year varchar(256),
            apply_form_current_occupation varchar(256),
            apply_form_work_ex varchar(256),
            user_placement_status text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with raw as 

        (select 
            courses_courseusermapping.id as course_user_mapping_id,
            courses_courseusermapping.user_id,
            courses_course.id as course_id,
            courses_course.title as course_name,
            courses_course.unit_type,
            courses_courseusermapping.admin_course_user_mapping_id,
            courses_courseusermapping.created_at as created_at,
            courses_courseusermapping.status, 
            courses_courseuserlabelmapping.id as label_id,
            (courses_courseusermapping.utm_param_json->'utm_source'::text) #>> '{}' as utm_source,
            (courses_courseusermapping.utm_param_json->'utm_medium'::text) #>> '{}' as utm_medium,
            (courses_courseusermapping.utm_param_json->'utm_campaign'::text) #>> '{}' as utm_campaign,
            courses_courseusermapping.hash,
            (SELECT U0.response FROM apply_forms_courseuserapplyformquestionmapping U0 WHERE (U0.apply_form_question_id = 17 AND U0.course_user_mapping_id = (courses_courseusermapping.id))  LIMIT 1) as apply_form_current_city,
            (SELECT U0.response FROM apply_forms_courseuserapplyformquestionmapping U0 WHERE (U0.apply_form_question_id = 3 AND U0.course_user_mapping_id = (courses_courseusermapping.id))  LIMIT 1) as apply_form_graduation_year,
            (SELECT U0.response FROM apply_forms_courseuserapplyformquestionmapping U0 WHERE (U0.apply_form_question_id = 53 AND U0.course_user_mapping_id = (courses_courseusermapping.id))  LIMIT 1) as apply_form_current_occupation,
            (SELECT U0.response FROM apply_forms_courseuserapplyformquestionmapping U0 WHERE (U0.apply_form_question_id = 54 AND U0.course_user_mapping_id = (courses_courseusermapping.id))  LIMIT 1) as apply_form_work_ex
        from
            courses_courseusermapping
        left join courses_courseuserlabelmapping
            on courses_courseuserlabelmapping.course_user_mapping_id = courses_courseusermapping.admin_course_user_mapping_id and courses_courseuserlabelmapping.label_id = 677
        left join courses_course 
            on courses_course.id = courses_courseusermapping.course_id),

    pr_npr_data as 
        (with au_user_raw as 
            (select
                courses_course.id as au_course_id,
                courses_course.title as au_batch_name,
                courses_courseusermapping.id as admin_cum_id,
                courses_courseusermapping.user_id,
                concat(auth_user.first_name,' ', auth_user.last_name) as student_name,
                courses_courseusermapping.status as admin_cum_status,
                case
                    when placements_courseuserplacementstatus.id is null or placements_courseuserplacementstatus.status = 1 then 'NPR'
                    when placements_courseuserplacementstatus.status = 2 then 'PR'
                    when placements_courseuserplacementstatus.status = 3 then 'Placed'
                end as user_placement_status
            from
                courses_courseusermapping
            join courses_course
                on courses_course.id = courses_courseusermapping.course_id and lower(courses_course.unit_type) like 'admin'
            left join placements_courseuserplacementstatus
                on placements_courseuserplacementstatus.course_user_mapping_id = courses_courseusermapping.id
            left join auth_user
                on auth_user.id = courses_courseusermapping.user_id and lower(auth_user.email) not like '%newtonschool.co%'),
                
        user_lu_au_mapping as
            (select
                courses_course.id as lu_cid,
                courses_course.title as lu_name,
                courses_courseusermapping.user_id,
                courses_courseusermapping.id as lu_cum_id,
                courses_courseusermapping.status as lu_cum_status,
                cc2.id as au_cid,
                cc2.title as au_batch_name,
                ccum2.user_id as au_uid,
                ccum2.id as au_cum_id,
                ccum2.status as au_cum_status
            from
                courses_course
            join courses_courseusermapping
                on courses_courseusermapping.course_id = courses_course.id and lower(courses_course.unit_type) like 'learning'
            left join courses_courseusermapping ccum2
                on ccum2.id = courses_courseusermapping.admin_course_user_mapping_id
            left join courses_course cc2
                on cc2.id = ccum2.course_id)
                
        select
            user_lu_au_mapping.*,
            au_user_raw.user_placement_status
        from
            user_lu_au_mapping
        left join au_user_raw
            on au_user_raw.admin_cum_id = user_lu_au_mapping.au_cum_id)
    select
        raw.course_user_mapping_id, 
        raw.user_id,
        raw.course_id,
        raw.course_name,
        raw.unit_type,
        raw.admin_course_user_mapping_id,
        r_one.course_name as admin_unit_name,
        r_one.course_id as admin_course_id,
        raw.created_at,
        raw.status, 
        raw.label_id,
        raw.utm_campaign,
        raw.utm_source,
        raw.utm_medium,
        raw.hash,
        raw.apply_form_current_city,
        raw.apply_form_graduation_year,
        raw.apply_form_current_occupation,
        raw.apply_form_work_ex,
        pr_npr_data.user_placement_status
        
    from
        raw
    left join raw as r_one
        on raw.admin_course_user_mapping_id = r_one.course_user_mapping_id
    left join pr_npr_data
        on pr_npr_data.user_id = raw.user_id and raw.course_id = pr_npr_data.lu_cid
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;
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