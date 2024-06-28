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


dag = DAG(
    'users_table_transformation_DAG',
    default_args=default_args,
    description='A DAG for users table transformation',
    schedule_interval='30 20 * * *',
    catchup=False
)


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS users_info (
            id serial,
            user_id bigint not null PRIMARY KEY,
            first_name varchar(100),
            last_name varchar(100),
            date_joined timestamp,
            last_login timestamp,
            username varchar(100),
            email varchar(100),
            phone varchar(16),
            current_location_city varchar(128),
            current_location_state varchar(128),
            gender varchar(10),
            date_of_birth date,
            utm_source varchar(256),
            utm_medium varchar(256),
            utm_campaign varchar(256),
            tenth_marks double precision,
            twelfth_marks double precision,
            bachelors_marks double precision,
            bachelors_grad_year date,
            bachelors_degree varchar(128),
            bachelors_field_of_study varchar(128),
            masters_marks double precision,
            masters_grad_year date,
            masters_degree varchar(128),
            masters_field_of_study varchar(128),
            lead_type varchar(32),
            course_structure_slug varchar(256),
            marketing_url_structure_slug varchar(256),
            signup_graduation_year int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with lead_type_table as(
            select distinct 
                user_id,
                case 
                    when other_status is null then 'Fresh'
                    else 'Deferred'
                end as lead_type
            
            from
            
                (with raw_data as
                
                    (select
                        courses_course.id as course_id,
                        courses_course.title as batch_name,
                        courses_course.start_timestamp,
                        courses_course.end_timestamp,
                        user_id,
                        courses_courseusermapping.status,
                        email
                    from
                        courses_courseusermapping
                    join courses_course
                        on courses_course.id = courses_courseusermapping.course_id and courses_courseusermapping.status not in (13,14,18,27) and courses_course.course_type in (1,6)
                    join auth_user
                        on auth_user.id = courses_courseusermapping.user_id
                    where date(courses_course.start_timestamp) < date(current_date)
                    and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,21,22,23,26,50,51,52,53,54,55,56,57,58,59,60)
                    and unit_type like 'LEARNING'),
                
                non_studying as 
                    (select 
                        * 
                    from
                        raw_data
                    where status in (11,30)),
                    
                studying as 
                    (select 
                        *
                    from
                        raw_data
                    where status in (5,8,9))
                    
                select
                    studying.*,
                    non_studying.status as other_status,
                    non_studying.course_id as other_course_id,
                    non_studying.start_timestamp as other_st,
                    non_studying.end_timestamp as other_et
                from
                    studying
                left join non_studying
                    on non_studying.user_id = studying.user_id
                group by 1,2,3,4,5,6,7,8,9,10,11) raw
            ),
            t1 as(
            select distinct auth_user.id as user_id,auth_user.first_name,auth_user.last_name,
                auth_user.date_joined as date_joined,
                auth_user.last_login as last_login,
                auth_user.username,
                auth_user.email,users_userprofile.phone,
                internationalization_city.name as current_location_city,
                internationalization_state.name as current_location_state,
                case when users_userprofile.gender = 1 then 'Male' when users_userprofile.gender = 2 then 'Female' 
                when users_userprofile.gender = 3 then 'Other' end as gender,
                users_userprofile.date_of_birth as date_of_birth,
                (users_userprofile.utm_param_json->'utm_source'::text) #>> '{}' as utm_source,
                (users_userprofile.utm_param_json->'utm_medium'::text) #>> '{}' as utm_medium,
                (users_userprofile.utm_param_json->'utm_campaign'::text) #>> '{}' as utm_campaign,
                A.grade as tenth_marks,B.grade as twelfth_marks,C.grade as bachelors_marks,
                C.end_date as bachelors_grad_year,
                E.name as bachelors_degree,F.name as bachelors_field_of_study,D.grade as masters_marks,
                D.end_date as masters_grad_year,M.name as masters_degree,MF.name as masters_field_of_study,
                lead_type_table.lead_type,
                (users_userprofile.utm_param_json->'course_structure_slug'::text) #>> '{}' as course_structure_slug,
                (users_userprofile.utm_param_json->'marketing_url_structure_slug'::text) #>> '{}' as marketing_url_structure_slug,
                users_extendeduserprofile.graduation_year as signup_graduation_year,
                row_number() over(partition by auth_user.id order by date_joined) as rank
                
                from auth_user left join users_userprofile on users_userprofile.user_id = auth_user.id 
                left join users_extendeduserprofile on users_extendeduserprofile.user_id = auth_user.id
                left join internationalization_city on users_userprofile.city_id = internationalization_city.id 
                left join internationalization_state on internationalization_state.id = internationalization_city.state_id
                FULL JOIN users_education A ON (A.user_id = auth_user.id AND A.education_type = 1) 
                FULL JOIN users_education B ON (B.user_id = auth_user.id AND B.education_type = 2 ) 
                FULL JOIN users_education C ON (C.user_id = auth_user.id AND C.education_type = 3) 
                FULL JOIN users_education D ON (D.user_id = auth_user.id AND D.education_type = 4) 
                left join education_degree E on C.degree_id = E.id  
                left join education_fieldofstudy F on C.field_of_study_id = F.id 
                left join education_degree M on D.degree_id = M.id  
                left join education_fieldofstudy MF on D.field_of_study_id = MF.id
                left join lead_type_table on lead_type_table.user_id = auth_user.id
                left join users_userentrylog on users_userentrylog.user_id = auth_user.id
                )
                select 
                    distinct user_id,first_name,last_name,date_joined,last_login,username,email,phone,current_location_city,current_location_state,gender,date_of_birth,utm_source,utm_medium,utm_campaign,
                    tenth_marks,twelfth_marks,bachelors_marks,bachelors_grad_year,bachelors_degree,bachelors_field_of_study,masters_marks,masters_grad_year,masters_degree,masters_field_of_study,lead_type,
                    course_structure_slug,marketing_url_structure_slug,signup_graduation_year
                from t1
                    where rank =1 and user_id is not null and last_login >= CURRENT_DATE - INTERVAL '7' DAY
                order by last_login desc;
        ''',
    dag=dag
)

def extract_exception_logs(**kwargs):
    transform_data_output = kwargs['ti'].xcom_pull(task_ids=extract_python_data)
    for logs in transform_data_output:
        print(logs)


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
    ctr = 0
    for transform_row in transform_data_output:
        print("Execution", ctr)
        ctr += 1
        pg_cursor.execute(
                'INSERT INTO users_info (user_id,first_name,last_name,date_joined,last_login,username,email,phone,'
                'current_location_city,current_location_state,gender,date_of_birth,utm_source,utm_medium,utm_campaign,'
                'tenth_marks,twelfth_marks,bachelors_marks,bachelors_grad_year,bachelors_degree,'
                'bachelors_field_of_study,masters_marks,masters_grad_year,masters_degree,masters_field_of_study,lead_type,'
                'course_structure_slug,marketing_url_structure_slug,signup_graduation_year) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
                'on conflict (user_id) do update set first_name=EXCLUDED.first_name,'
                'last_name=EXCLUDED.last_name,last_login=EXCLUDED.last_login,'
                'username=EXCLUDED.username,email=EXCLUDED.email,phone=EXCLUDED.phone,'
                'current_location_city=EXCLUDED.current_location_city,current_location_state=EXCLUDED.current_location_state,'
                'gender=EXCLUDED.gender,date_of_birth=EXCLUDED.date_of_birth,'
                'utm_source=EXCLUDED.utm_source,utm_medium=EXCLUDED.utm_medium,utm_campaign=EXCLUDED.utm_campaign,'
                'tenth_marks=EXCLUDED.tenth_marks,twelfth_marks=EXCLUDED.twelfth_marks,'
                'bachelors_marks=EXCLUDED.bachelors_marks,bachelors_grad_year=EXCLUDED.bachelors_grad_year,'
                'bachelors_degree=EXCLUDED.bachelors_degree,bachelors_field_of_study=EXCLUDED.bachelors_field_of_study,'
                'masters_marks=EXCLUDED.masters_marks,masters_grad_year=EXCLUDED.masters_grad_year,'
                'masters_degree=EXCLUDED.masters_degree,masters_field_of_study=EXCLUDED.masters_field_of_study,'
                'lead_type=EXCLUDED.lead_type,course_structure_slug=EXCLUDED.course_structure_slug,'
                'marketing_url_structure_slug=EXCLUDED.marketing_url_structure_slug,'
                'signup_graduation_year=EXCLUDED.signup_graduation_year ;',
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
                )
        )
    pg_conn.commit()

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

# extract_data = PostgresOperator(
#     task_id='extract_data',
#     postgres_conn_id='postgres_result_db',
#     sql='''SELECT * FROM {{ task_instance.xcom_pull(task_ids='transform_data') }}''',
#     dag=dag
# )

create_table >> transform_data >> extract_python_data
