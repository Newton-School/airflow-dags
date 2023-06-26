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
            'INSERT INTO arl_user_ratings (table_unique_key,student_id,course_id,'
            'course_user_mapping_status, label_mapping_status, course_name,topic_pool_id,'
            'template_name,rating,plagiarised_rating,mock_rating,module_cutoff,'
            'required_rating,grade_obtained,assignment_rating,contest_rating,'
            'milestone_rating,proctored_contest_rating,quiz_rating,plagiarised_assignment_rating,'
            'plagiarised_contest_rating,plagiarised_proctored_contest_rating)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_id = EXCLUDED.student_id,'
            'course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'course_user_mapping_status = EXCLUDED.course_user_mapping_status,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'topic_pool_id = EXCLUDED.topic_pool_id,'
            'template_name = EXCLUDED.template_name,'
            'rating = EXCLUDED.rating,'
            'plagiarised_rating = EXCLUDED.plagiarised_rating,'
            'mock_rating = EXCLUDED.mock_rating,'
            'module_cutoff = EXCLUDED.module_cutoff,'
            'required_rating = EXCLUDED.required_rating,'
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
                transform_row[21]
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_user_ratings_dag',
    default_args=default_args,
    description='Processed version of user_ratings table with grade_obtained column definition for this column given by the program team',
    schedule_interval='35 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_user_ratings (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            student_id bigint,
            course_id bigint,
            course_name varchar(128),
            course_user_mapping_status int,
            label_mapping_status varchar(32),
            topic_pool_id int,
            template_name varchar(128),
            rating int,
            plagiarised_rating int,
            mock_rating int,
            module_cutoff int,
            required_rating int,
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
        cast(concat(course_id, topic_pool_id, student_id, rating, course_user_mapping_status, topic_pool_id) as double precision) as table_unique_key,
        student_id,
        course_id,
        course_name,
        course_user_mapping_status,
        case 
        	when label_id is not null and course_user_mapping_status in (5,8,9) then 'Label Marked Student'
    		when label_id is null and course_user_mapping_status in (5,8,9) then 'Enrolled Student'
    		else 'Other'
        end as label_mapping_status,
        topic_pool_id,
        template_name,
        rating,
        plagiarised_rating,
        mock_rating,
        module_cutoff,
        required_rating,
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
    (SELECT
        b.student_id,
        courses.course_id, 
        courses.course_name,
        cum.status as course_user_mapping_status,
        cum.label_id,
        topic_pool_id,
        template_name,
        max(b.rating) as rating,
        max(b.plagiarised_rating) as plagiarised_rating,
        max(b.mock_rating) as mock_rating,
        max(b.module_cutoff) as module_cutoff,
        (max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) as required_rating,
        CASE
        WHEN (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) >= 0.9 THEN 'A'
        WHEN (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) >= 0.6 AND (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) < 0.9 THEN 'B'
        WHEN (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) >= 0.3 AND (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) < 0.6 THEN 'C'
        WHEN (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) >= 0 AND (1.0*(max(b.rating) - max(b.plagiarised_rating) - max(b.mock_rating)) / max(b.module_cutoff)) < 0.3 THEN 'D'
    END AS grade_obtained,
    max(assignment_rating) as assignment_rating,
    max(contest_rating) as contest_rating,
    max(milestone_rating) as milestone_rating,
    max(proctored_contest_rating) as proctored_contest_rating,
    max(quiz_rating) as quiz_rating,
    max(plagiarised_assignment_rating) as plagiarised_assignment_rating,
    max(plagiarised_contest_rating) as plagiarised_contest_rating,
    max(plagiarised_proctored_contest_rating) as plagiarised_proctored_contest_rating
    
FROM 
    (SELECT 
        ur.student_id,
        ur.created_at,
        ur.topic_pool_id,
        ur.template_name,
        CASE
            WHEN template_name LIKE 'LINEAR DSA 1' THEN 550
            WHEN template_name LIKE 'LINEAR DSA 2' THEN 150
            WHEN template_name LIKE 'Frontend Track - HTML,CSS' THEN 275
            WHEN template_name LIKE 'Backend Track -> Node, Express, Mongo, SQL, System Design' THEN 250
            WHEN template_name LIKE 'React' THEN 250
            WHEN template_name LIKE 'JS' THEN 250
            WHEN template_name LIKE 'NON-LINEAR DSA 1' THEN 100
            WHEN template_name LIKE 'NON-LINEAR DSA 2' THEN 100
            WHEN template_name LIKE 'NON-LINEAR DSA 3' THEN 100
            ELSE 0
        END AS module_cutoff,
        ur.rating,
        ur.assignment_rating,
        ur.contest_rating,
        ur.milestone_rating,
        ur.mock_rating,
        ur.proctored_contest_rating,
        ur.quiz_rating,
        ur.plagiarised_assignment_rating,
        ur.plagiarised_contest_rating,
        ur.plagiarised_proctored_contest_rating,
        ur.plagiarised_rating
    FROM 
        user_ratings ur) AS b
left JOIN course_user_mapping cum
    ON b.student_id = cum.user_id
join courses
    on courses.course_id = cum.course_id and courses.unit_type is not null
where module_cutoff <> 0
group by 1,2,3,4,5,6,7) all_combined;
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