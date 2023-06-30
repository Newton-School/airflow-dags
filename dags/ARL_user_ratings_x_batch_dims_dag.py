from airflow import DAG
# from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

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
            'INSERT INTO user_ratings_x_batch_dims (table_unique_key,'
            'user_id,'
            'student_name,'
            'date_joined,'
            'last_login,'
            'username,'
            'email,'
            'phone,'
            'current_location_city,'
            'current_location_state,'
            'gender,'
            'date_of_birth,'
            'utm_source,'
            'utm_medium,'
            'utm_campaign,'
            'tenth_marks,'
            'twelfth_marks,'
            'bachelors_marks,'
            'bachelors_grad_year,'
            'bachelors_field_of_study,'
            'masters_marks,'
            'masters_grad_year,'
            'masters_degree,'
            'masters_field_of_study,'
            'lead_type,'
            'topic_pool_id,'
            'template_name,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'course_start_date,'
            'course_age,'
            'status,'
            'label_mapping_id,'
            'module_cutoff,'
            'rating,'
            'mock_rating,'
            'plagiarised_rating,'
            'required_rating,'
            'module_percent,'
            'grade_obtained,'
            'assignment_rating,'
            'contest_rating,'
            'milestone_rating,'
            'proctored_contest_rating,'
            'quiz_rating,'
            'plagiarised_assignment_rating,'
            'plagiarised_contest_rating,'
            'plagiarised_proctored_contest_rating)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set user_id = EXCLUDED.user_id,'
            'student_name = EXCLUDED.student_name,'
            'date_joined = EXCLUDED.date_joined,'
            'last_login = EXCLUDED.last_login,'
            'username = EXCLUDED.username,'
            'email = EXCLUDED.email,'
            'phone = EXCLUDED.phone,'
            'current_location_city = EXCLUDED.current_location_city,'
            'current_location_state = EXCLUDED.current_location_state,'
            'gender = EXCLUDED.gender,'
            'date_of_birth = EXCLUDED.date_of_birth,'
            'utm_source = EXCLUDED.utm_source,'
            'utm_medium = EXCLUDED.utm_medium,'
            'utm_campaign = EXCLUDED.utm_campaign,'
            'tenth_marks = EXCLUDED.tenth_marks,'
            'twelfth_marks = EXCLUDED.twelfth_marks,'
            'bachelors_marks = EXCLUDED.bachelors_marks,'
            'bachelors_grad_year = EXCLUDED.bachelors_grad_year,'
            'bachelors_field_of_study = EXCLUDED.bachelors_field_of_study,'
            'masters_marks = EXCLUDED.masters_marks,'
            'masters_grad_year = EXCLUDED.masters_grad_year,'
            'masters_degree = EXCLUDED.masters_degree,'
            'masters_field_of_study = EXCLUDED.masters_field_of_study,'
            'lead_type = EXCLUDED.lead_type,'
            'topic_pool_id = EXCLUDED.topic_pool_id,'
            'template_name = EXCLUDED.template_name,'
            'course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'course_start_date = EXCLUDED.course_start_date,'
            'course_age = EXCLUDED.course_age,'
            'status = EXCLUDED.status,'
            'label_mapping_id = EXCLUDED.label_mapping_id,'
            'module_cutoff = EXCLUDED.module_cutoff,'
            'rating = EXCLUDED.rating,'
            'mock_rating = EXCLUDED.mock_rating,'
            'plagiarised_rating = EXCLUDED.plagiarised_rating,'
            'required_rating = EXCLUDED.required_rating,'
            'module_percent = EXCLUDED.module_percent,'
            'grade_obtained = EXCLUDED.grade_obtained,'
            'assignment_rating = EXCLUDED.assignment_rating,'
            'contest_rating = EXCLUDED.contest_rating,'
            'milestone_rating = EXCLUDED.milestone_rating,'
            'proctored_contest_rating = EXCLUDED.proctored_contest_rating,'
            'quiz_rating = EXCLUDED.quiz_rating,'
            'plagiarised_assignment_rating = EXCLUDED.plagiarised_assignment_rating,'
            'plagiarised_contest_rating = EXCLUDED.plagiarised_contest_rating,'
            'plagiarised_proctored_contest_rating = EXCLUDED.plagiarised_proctored_contest_rating;',
            (
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
                transform_row[33],
                transform_row[34],
                transform_row[35],
                transform_row[36],
                transform_row[37],
                transform_row[38],
                transform_row[39],
                transform_row[40],
                transform_row[41],
                transform_row[42],
                transform_row[43],
                transform_row[44],
                transform_row[45],
                transform_row[46],
                transform_row[47],
                transform_row[48]
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_user_x_batches_dims_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='This DAG provides user ratings per module per batch',
    schedule_interval='35 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS user_ratings_x_batch_dims (
            id serial,
            table_unique_key varchar(256) not null PRIMARY KEY,
            user_id bigint,
            student_name varchar(256),
            date_joined timestamp,
            last_login timestamp,
            username varchar(256),
            email varchar(256),
            phone varchar(128),
            current_location_city varchar(256),
            current_location_state varchar(256),
            gender varchar(16),
            date_of_birth DATE,
            utm_source varchar(256),
            utm_medium varchar(256),
            utm_campaign varchar(256),
            tenth_marks real,
            twelfth_marks real,
            bachelors_marks real,
            bachelors_grad_year DATE,
            bachelors_field_od_study varchar(128),
            masters_marks real,
            masters_grad_year DATE,
            masters_degree varchar(128),
            masters_field_of_study varchar(128),
            lead_type varchar(32),
            topic_pool_id int,
            template_name varchar(256),
            course_id int,
            course_name varchar(256),
            course_structure_class varchar(128),
            course_start_date DATE,
            course_age int,
            status int,
            label_mapping_id bigint,
            module_cutoff int,
            rating int,
            mock_rating int,
            plagiarised_rating int,
            required_rating int,
            module_percent real,
            grade_obtained varchar(8),
            assignment_rating int,
            contest_rating int,
            milestone_rating int,
            proctored_contest_rating int,
            quiz_rating int,
            plagiarised_assignment_rating int,
            plagiarised_contest_rating int,
            plagiarised_proctored_contest_rating int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
    select 
        concat(user_id,course_id, topic_pool_id,status) as table_unique_key, 
        user_id,
        student_name,
        date_joined,
        last_login,
        username,
        email,
        phone,
        current_location_city,
        current_location_state,
        gender,
        date_of_birth,
        utm_source,
        utm_medium,
        utm_campaign,
        tenth_marks,
        twelfth_marks,
        bachelors_marks,
        bachelors_grad_year,
        bachelors_field_of_study,
        masters_marks,
        masters_grad_year,
        masters_degree,
        masters_field_of_study,
        lead_type,
        topic_pool_id,
        template_name,
        course_id,
        course_name,
        course_structure_class,
        course_start_date,
        course_age,
        status,
        label_mapping_id,
        module_cutoff,
        rating,
        mock_rating,
        plagiarised_rating,
        required_rating,
        module_percent,
        grade_obtained,
        assignment_rating,
        contest_rating,
        milestone_rating,
        proctored_contest_rating,
        quiz_rating,
        plagiarised_assignment_rating,
        plagiarised_contest_rating,
        plagiarised_proctored_contest_rating
    from
        (select 
            users_info.user_id,
            concat(users_info.first_name,' ', users_info.last_name) as student_name,
            users_info.date_joined,
            users_info.last_login,
            users_info.username,
            users_info.email,
            users_info.phone,
            users_info.current_location_city,
            users_info.current_location_state,
            users_info.gender,
            users_info.date_of_birth,
            users_info.utm_source,
            users_info.utm_medium,
            users_info.utm_campaign,
            users_info.tenth_marks,
            users_info.twelfth_marks,
            users_info.bachelors_marks,
            users_info.bachelors_grad_year,
            users_info.bachelors_field_of_study,
            users_info.masters_marks,
            users_info.masters_grad_year,
            users_info.masters_degree,
            users_info.masters_field_of_study,
            users_info.lead_type,
            user_ratings.topic_pool_id,
            user_ratings.template_name,
            wud.course_id,
            acd.course_name,
            acd.course_structure_class,
            acd.course_start_date,
            acd.course_age,
            wud.status,
            wud.label_mapping_id,
            CASE
                    WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                    WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                    WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                    WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                    WHEN user_ratings.template_name LIKE 'React' THEN 250
                    WHEN user_ratings.template_name LIKE 'JS' THEN 250
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                    ELSE 0
                END AS module_cutoff,
            max(rating) as rating,
            max(mock_rating) as mock_rating,
            max(plagiarised_rating) as plagiarised_rating,
            max(rating) - max(plagiarised_rating) - max(mock_rating) as required_rating,
            (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                    WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                    WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                    WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                    WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                    WHEN user_ratings.template_name LIKE 'React' THEN 250
                    WHEN user_ratings.template_name LIKE 'JS' THEN 250
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                    WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                end) as module_percent,
            case 	
                        when (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) >= 0.9 then 'A'
                        when (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) >= 0.6 and (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) < 0.9 then 'B'
                        when (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) >= 0.3 and (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) < 0.6 then 'C'
                        when (max(rating) - max(plagiarised_rating) - max(mock_rating))*1.0 / (CASE
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 1' THEN 550
                            WHEN user_ratings.template_name LIKE 'LINEAR DSA 2' THEN 150
                            WHEN user_ratings.template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
                            WHEN user_ratings.template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
                            WHEN user_ratings.template_name LIKE 'React' THEN 250
                            WHEN user_ratings.template_name LIKE 'JS' THEN 250
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 1' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 2' THEN 100
                            WHEN user_ratings.template_name LIKE 'NON-LINEAR DSA 3' THEN 100
                        end) < 0.3 then 'D'
            end as grade_obtained,
            max(assignment_rating) as assignment_rating,
            max(contest_rating) as contest_rating,
            max(milestone_rating) as milestone_rating,
            max(proctored_contest_rating) as proctored_contest_rating,
            max(quiz_rating) as quiz_rating,
            max(plagiarised_assignment_rating) as plagiarised_assignment_rating,
            max(plagiarised_contest_rating) as plagiarised_contest_rating,
            max(plagiarised_proctored_contest_rating) as plagiarised_proctored_contest_rating,
            dense_rank () over (partition by users_info.user_id, wud.course_id order by wud.label_mapping_id) as d_rank
        from
            weekly_user_details wud
        join users_info
            on wud.user_id = users_info.user_id and wud.status in (8,9) and wud.course_id > 200
        left join user_ratings
            on user_ratings.student_id= users_info.user_id and user_ratings.topic_pool_id in (145,146,147,150,173,175,177,178,179,180)
        left join arl_course_dimensions acd
            on acd.course_id = wud.course_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34) main_q
where d_rank = 1;
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