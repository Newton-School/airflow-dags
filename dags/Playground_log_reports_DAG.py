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
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
                'INSERT INTO playground_log_reports (table_unique_key,'
                'playground_id,'
                'playground_type,'
                'created_at,'
                'assignment_question_id,'
                'relation_with_playground,'
                'user_id,'
                'assignment_id,'
                'course_id,'
                'time_spent_in_seconds) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set time_spent_in_seconds = EXCLUDED.time_spent_in_seconds;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'playground_log_report_DAG',
    default_args=default_args,
    description='Table has per user per playground total time spent',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS playground_log_reports (
            id serial,
            table_unique_key text NOT NULL PRIMARY KEY,
            playground_id bigint,
            playground_type text,
            created_at timestamp,
            assignment_question_id bigint,
            relation_with_playground text,
            user_id bigint,
            assignment_id bigint,            
            course_id int,
            time_spent_in_seconds real 
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
   concat(playground_id,'_',user_id,'_',assignment_question_id) as table_unique_key,
   *
from 
    (select 
        assignments_assignmentcourseuserquestionmapping.coding_playground_id as playground_id,
        'coding_playground' as playgrond_type,
        playgrounds_playgroundlogreport.created_at,
        assignments_assignmentcourseuserquestionmapping.assignment_question_id,
        case
            when playgrounds_playgroundlogreport.relation_with_playground = 1 then 'Owner'
            when playgrounds_playgroundlogreport.relation_with_playground = 2 then 'Mentor'
            when playgrounds_playgroundlogreport.relation_with_playground = 3 then 'Other'
        end as relation_with_playground,
        playgrounds_playgroundlogreport.user_id,
        assignments_assignmentcourseusermapping.assignment_id,
        courses_courseusermapping.course_id,
        extract(epoch from playgrounds_playgroundlogreport.total_time) as time_spent_in_seconds
    from
        playgrounds_playgroundlogreport
    join assignments_assignmentcourseuserquestionmapping
        on assignments_assignmentcourseuserquestionmapping.coding_playground_id = playgrounds_playgroundlogreport.object_id
            and playgrounds_playgroundlogreport.content_type_id = 69 and playgrounds_playgroundlogreport.created_at >= '2023-01-01'
    join assignments_assignmentcourseusermapping
        on assignments_assignmentcourseusermapping.id = assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id
    join courses_courseusermapping
        on courses_courseusermapping.id = assignments_assignmentcourseusermapping.course_user_mapping_id
            and courses_courseusermapping.user_id = playgrounds_playgroundlogreport.user_id
                and course_id > 200
    
    
    UNION 
    
    select 
        assignments_assignmentcourseuserquestionmapping.front_end_playground_id,
        'frontend_playground' as playgrond_type,
        playgrounds_playgroundlogreport.created_at,
        assignments_assignmentcourseuserquestionmapping.assignment_question_id,
        case
            when playgrounds_playgroundlogreport.relation_with_playground = 1 then 'Owner'
            when playgrounds_playgroundlogreport.relation_with_playground = 2 then 'Mentor'
            when playgrounds_playgroundlogreport.relation_with_playground = 3 then 'Other'
        end as relation_with_playground,
        courses_courseusermapping.user_id,
        assignments_assignmentcourseusermapping.assignment_id,
        courses_courseusermapping.course_id,
        extract(epoch from playgrounds_playgroundlogreport.total_time) as time_spent_in_seconds
    from
        playgrounds_playgroundlogreport
    join assignments_assignmentcourseuserquestionmapping
        on assignments_assignmentcourseuserquestionmapping.front_end_playground_id = playgrounds_playgroundlogreport.object_id
            and playgrounds_playgroundlogreport.content_type_id = 59 and playgrounds_playgroundlogreport.created_at >= '2023-01-01'
    join assignments_assignmentcourseusermapping
        on assignments_assignmentcourseusermapping.id = assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id
    join courses_courseusermapping
        on courses_courseusermapping.id = assignments_assignmentcourseusermapping.course_user_mapping_id
            and course_id > 200 and courses_courseusermapping.user_id = playgrounds_playgroundlogreport.user_id
            
    UNION 
    
    select 
        assignments_assignmentcourseuserquestionmapping.game_playground_id,
        'game_playground' as playgrond_type,
        playgrounds_playgroundlogreport.created_at,
        assignments_assignmentcourseuserquestionmapping.assignment_question_id,
        case
            when playgrounds_playgroundlogreport.relation_with_playground = 1 then 'Owner'
            when playgrounds_playgroundlogreport.relation_with_playground = 2 then 'Mentor'
            when playgrounds_playgroundlogreport.relation_with_playground = 3 then 'Other'
        end as relation_with_playground,
        courses_courseusermapping.user_id,
        assignments_assignmentcourseusermapping.assignment_id,
        courses_courseusermapping.course_id,
        extract(epoch from playgrounds_playgroundlogreport.total_time) as time_spent_in_seconds
    from
        playgrounds_playgroundlogreport
    join assignments_assignmentcourseuserquestionmapping
        on assignments_assignmentcourseuserquestionmapping.game_playground_id = playgrounds_playgroundlogreport.object_id
            and playgrounds_playgroundlogreport.content_type_id = 178 and playgrounds_playgroundlogreport.created_at >= '2023-01-01'
    join assignments_assignmentcourseusermapping
        on assignments_assignmentcourseusermapping.id = assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id
    join courses_courseusermapping
        on courses_courseusermapping.id = assignments_assignmentcourseusermapping.course_user_mapping_id
            and course_id > 200 and courses_courseusermapping.user_id = playgrounds_playgroundlogreport.user_id
    
    UNION 
    
    select 
        assignments_assignmentcourseuserquestionmapping.project_playground_id,
        'project_playground' as playgrond_type,
        playgrounds_playgroundlogreport.created_at,
        assignments_assignmentcourseuserquestionmapping.assignment_question_id,
        case
            when playgrounds_playgroundlogreport.relation_with_playground = 1 then 'Owner'
            when playgrounds_playgroundlogreport.relation_with_playground = 2 then 'Mentor'
            when playgrounds_playgroundlogreport.relation_with_playground = 3 then 'Other'
        end as relation_with_playground,
        courses_courseusermapping.user_id,
        assignments_assignmentcourseusermapping.assignment_id,
        courses_courseusermapping.course_id,
        extract(epoch from playgrounds_playgroundlogreport.total_time) as time_spent_in_seconds
    from
        playgrounds_playgroundlogreport
    join assignments_assignmentcourseuserquestionmapping
        on assignments_assignmentcourseuserquestionmapping.project_playground_id = playgrounds_playgroundlogreport.object_id
            and playgrounds_playgroundlogreport.content_type_id = 81 and playgrounds_playgroundlogreport.created_at >= '2023-01-01'
    join assignments_assignmentcourseusermapping
        on assignments_assignmentcourseusermapping.id = assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id
    join courses_courseusermapping
        on courses_courseusermapping.id = assignments_assignmentcourseusermapping.course_user_mapping_id
            and course_id > 200 and courses_courseusermapping.user_id = playgrounds_playgroundlogreport.user_id
            
    UNION 
    
    select 
        assignments_assignmentcourseuserquestionmapping.subjective_id,
        'subject_playground' as playgrond_type,
        playgrounds_playgroundlogreport.created_at,
        assignments_assignmentcourseuserquestionmapping.assignment_question_id,
        case
            when playgrounds_playgroundlogreport.relation_with_playground = 1 then 'Owner'
            when playgrounds_playgroundlogreport.relation_with_playground = 2 then 'Mentor'
            when playgrounds_playgroundlogreport.relation_with_playground = 3 then 'Other'
        end as relation_with_playground,
        courses_courseusermapping.user_id,
        assignments_assignmentcourseusermapping.assignment_id,
        courses_courseusermapping.course_id,
        extract(epoch from playgrounds_playgroundlogreport.total_time) as time_spent_in_seconds
    from
        playgrounds_playgroundlogreport
    join assignments_assignmentcourseuserquestionmapping
        on assignments_assignmentcourseuserquestionmapping.subjective_id = playgrounds_playgroundlogreport.object_id
            and playgrounds_playgroundlogreport.content_type_id = 65 and playgrounds_playgroundlogreport.created_at >= '2023-01-01'
    join assignments_assignmentcourseusermapping
        on assignments_assignmentcourseusermapping.id = assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id
    join courses_courseusermapping
        on courses_courseusermapping.id = assignments_assignmentcourseusermapping.course_user_mapping_id
            and course_id > 200 and courses_courseusermapping.user_id = playgrounds_playgroundlogreport.user_id) a;
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