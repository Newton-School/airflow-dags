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
            'INSERT INTO arl_user_ratings (table_unique_key, user_id,'
            'course_structure_id, course_structure_class,'
            'course_id, course_name, course_user_mapping_status, label_mapping_status ,topic_pool_id,'
            'template_name, module_cutoff, rating, plagiarised_rating, mock_rating,'
            'required_rating, grade_obtained, assignment_rating, contest_rating,'
            'milestone_rating, proctored_contest_rating, quiz_rating, plagiarised_assignment_rating,'
            'plagiarised_contest_rating, plagiarised_proctored_contest_rating)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_structure_id = EXCLUDED.course_structure_id,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'course_name = EXCLUDED.course_name,'
            'course_user_mapping_status = EXCLUDED.course_user_mapping_status,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'topic_pool_id = EXCLUDED.topic_pool_id,'
            'template_name = EXCLUDED.template_name,'
            'module_cutoff = EXCLUDED.module_cutoff,'
            'rating = EXCLUDED.rating,'
            'plagiarised_rating = EXCLUDED.plagiarised_rating,'
            'mock_rating = EXCLUDED.mock_rating,'
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
                transform_row[21],
                transform_row[22],
                transform_row[23],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_user_ratings_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Processed version of user_ratings table with grade_obtained column definition for this column given by the program team',
    schedule_interval='45 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_user_ratings (
            id serial,
            table_unique_key varchar(255) not null PRIMARY KEY,
            user_id bigint,
            course_structure_id int,
            course_structure_class varchar(255),
            course_id bigint,
            course_name varchar(255),
            course_user_mapping_status int,
            label_mapping_status varchar(255),
            topic_pool_id int,
            template_name varchar(255),
            module_cutoff int,
            rating int,
            plagiarised_rating int,
            mock_rating int,
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
        with processed_data as 
            (select 
                student_id,
                topic_pool_id,
                template_name,
                cum.status,
                cum.label_id,
                c.course_id,
                c.course_name,
                c.course_structure_id,
                c.course_structure_class,
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
                max(rating) as rating,
                max(plagiarised_rating) as plagiarised_rating,
                max(mock_rating) as mock_rating,
                (max(rating) - max(plagiarised_rating) - max(mock_rating)) as required_rating,
                max(assignment_rating) as assignment_rating,
                max(contest_rating) as contest_rating,
                max(milestone_rating) as milestone_rating,
                max(proctored_contest_rating) as proctored_contest_rating,
                max(quiz_rating) as quiz_rating,
                max(plagiarised_assignment_rating) as plagiarised_assignment_rating,
                max(plagiarised_contest_rating) as plagiarised_contest_rating,
                max(plagiarised_proctored_contest_rating) as plagiarised_proctored_contest_rating	
            from
                user_ratings ur 
            join course_user_mapping cum 
                on cum.user_id = ur.student_id and cum.status in (8,9,11,12,30)
            join courses c 
                on c.course_id = cum.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26,32)
                    and lower(c.unit_type) like 'learning'
            group by 1,2,3,4,5,6,7,8,9,10)
            
        select
            concat(student_id, course_id, topic_pool_id) as table_unique_key,
            student_id as user_id,
            course_structure_id,
            course_structure_class,
            course_id,
            course_name,
            status as course_user_mapping_status,
            case 
                when label_id is null and status in (8,9) then 'Enrolled Student'
                when label_id is not null and status in (8,9) then 'Label Marked Student'
                when course_structure_id in (1,18) and status in (11,12) then 'ISA Cancelled Student'
                when course_structure_id not in (1,18) and status in (30) then 'Deferred Student'
                when course_structure_id not in (1,18) and status in (11) then 'Foreclosed Student'
                when course_structure_id not in (1,18) and status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            topic_pool_id,
            template_name,
            module_cutoff,
            rating,
            plagiarised_rating,
            mock_rating,
            required_rating,
            case 
                when required_rating * 1.0 / module_cutoff >= 0.9 then 'A'
                when required_rating * 1.0 / module_cutoff >= 0.6 and required_rating * 1.0 / module_cutoff < 0.9 then 'B'
                when required_rating * 1.0 / module_cutoff >= 0.3 and required_rating * 1.0 / module_cutoff < 0.6 then 'C'
                when required_rating * 1.0 / module_cutoff < 0.3 then 'D'
            end as grade_obtained,
            assignment_rating,
            contest_rating,
            milestone_rating,
            proctored_contest_rating,
            quiz_rating,
            plagiarised_assignment_rating,
            plagiarised_contest_rating,
            plagiarised_proctored_contest_rating
        from
            processed_data
        where module_cutoff <> 0;
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