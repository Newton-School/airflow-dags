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


def extract_exception_logs(**kwargs):
    transform_data_output = kwargs['ti'].xcom_pull(task_ids=extract_python_data)
    for logs in transform_data_output:
        print(logs)


def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    sql_execution_errors = []
    for transform_row in transform_data_output:
        try:
            pg_cursor.execute(
                    'INSERT INTO users_info (user_id,first_name,last_name,date_joined,last_login,username,email,phone,'
                    'current_location,gender,date_of_birth,utm_source,utm_medium,utm_campaign,'
                    'tenth_marks,twelfth_marks,bachelors_marks,bachelors_grad_year,bachelors_degree,'
                    'bachelors_field_of_study,masters_marks,masters_grad_year,masters_degree,masters_field_of_study) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
                    'on conflict (user_id) do update set last_login = EXCLUDED.last_login ;',
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
        except Exception as err:
            sql_execution_errors.append(str(err))
        # insert_query = f'INSERT INTO user_details_test (user_id,username,email,name,phone,last_login) VALUES ' \
        #                f'(' \
        #                f'{clean_input("int",transform_row[0])},' \
        #                f'{clean_input("string",transform_row[1])},' \
        #                f'{clean_input("string",transform_row[2])},' \
        #                f'{clean_input("string",transform_row[3])},' \
        #                f'{clean_input("string",transform_row[4])},' \
        #                f'{clean_input("datetime",transform_row[5])}' \
        #                f');'
        # pg_hook.run(insert_query)
    pg_conn.commit()
    return sql_execution_errors


dag = DAG(
    'users_table_transformation_DAG',
    default_args=default_args,
    description='A DAG for users table transformation',
    schedule_interval='0 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS users_info (
            user_id bigint not null PRIMARY KEY,
            first_name varchar(100),
            last_name varchar(100),
            date_joined timestamp,
            last_login timestamp,
            username varchar(100),
            email varchar(100),
            phone varchar(16),
            current_location varchar(100),
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
            masters_field_of_study varchar(128)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with t1 as(
select distinct auth_user.id as user_id,auth_user.first_name,auth_user.last_name,
    cast(auth_user.date_joined as varchar) as date_joined,cast(auth_user.last_login as varchar) as last_login,
    auth_user.username,
    auth_user.email,users_userprofile.phone,internationalization_city.name as current_location,
    case when users_userprofile.gender = 1 then 'Male' when users_userprofile.gender = 2 then 'Female' 
    when users_userprofile.gender = 3 then 'Other' end as gender,
    cast(users_userprofile.date_of_birth as varchar) as date_of_birth,
    (users_userprofile.utm_param_json->'utm_source'::text) #>> '{}' as utm_source,
    (users_userprofile.utm_param_json->'utm_medium'::text) #>> '{}' as utm_medium,
    (users_userprofile.utm_param_json->'utm_campaign'::text) #>> '{}' as utm_campaign,
    A.grade as tenth_marks,B.grade as twelfth_marks,C.grade as bachelors_marks,
    cast(C.end_date as varchar) as bachelors_grad_year,
    E.name as bachelors_degree,F.name as bachelors_field_of_study,D.grade as masters_marks,
    cast(D.end_date as varchar) as masters_grad_year,M.name as masters_degree,MF.name as masters_field_of_study,
    row_number() over(partition by auth_user.id order by date_joined) as rank
    
    from auth_user left join users_userprofile on users_userprofile.user_id = auth_user.id 
    left join internationalization_city on users_userprofile.city_id = internationalization_city.id 
    FULL JOIN users_education A ON (A.user_id = auth_user.id AND A.education_type = 1) 
    FULL JOIN users_education B ON (B.user_id = auth_user.id AND B.education_type = 2 ) 
    FULL JOIN users_education C ON (C.user_id = auth_user.id AND C.education_type = 3) 
    FULL JOIN users_education D ON (D.user_id = auth_user.id AND D.education_type = 4) 
    left join education_degree E on C.degree_id = E.id  
    left join education_fieldofstudy F on C.field_of_study_id = F.id 
    left join education_degree M on D.degree_id = M.id  
    left join education_fieldofstudy MF on D.field_of_study_id = MF.id
    )
    select 
        distinct user_id,first_name,last_name,date_joined,last_login,username,email,phone,current_location,gender,
        date_of_birth,utm_source,utm_medium,utm_campaign,
        tenth_marks,twelfth_marks,bachelors_marks,bachelors_grad_year,bachelors_degree,bachelors_field_of_study,masters_marks,masters_grad_year,masters_degree,masters_field_of_study
    from t1
        where rank =1 and user_id is not null;
        ''',
    dag=dag
)

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
