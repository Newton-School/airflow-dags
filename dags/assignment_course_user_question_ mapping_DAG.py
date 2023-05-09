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
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}
assignment_per_dags = Variable.get("assignment_per_dag", 40)
total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 500)

def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_assignment_sub_dag_id = kwargs['current_assignment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(task_ids=f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(
                 'INSERT INTO assignment_question_user_mapping (table_unique_key,user_id,assignment_id,question_id,question_started_at,'
            'question_completed_at,completed,all_test_case_passed,playground_type,playground_id,hash,'
            'latest_assignment_question_hint_mapping_id,late_submission,max_test_case_passed,assignment_started_at,'
            'assignment_completed_at,assignment_cheated_marked_at,cheated,plagiarism_submission_id,'
            'plagiarism_score,solution_length,number_of_submissions,error_faced_count) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (uaq_id) do update set question_completed_at = EXCLUDED.question_completed_at,'
            'completed=EXCLUDED.completed, all_test_case_passed=EXCLUDED.all_test_case_passed, latest_assignment_question_hint_mapping_id=EXCLUDED.latest_assignment_question_hint_mapping_id,'
            'late_submission=EXCLUDED.late_submission, max_test_case_passed=EXCLUDED.max_test_case_passed, assignment_completed_at=EXCLUDED.assignment_completed_at,'
            'assignment_cheated_marked_at=EXCLUDED.assignment_cheated_marked_at, cheated=EXCLUDED.cheated,'
            'plagiarism_submission_id=EXCLUDED.plagiarism_submission_id,plagiarism_score=EXCLUDED.plagiarism_score,'
            'solution_length=EXCLUDED.solution_length, number_of_submissions=EXCLUDED.number_of_submissions,'
            'error_faced_count=EXCLUDED.error_faced_count ;',
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
                 )
        )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()



dag = DAG(
    'assignment_question_user_mapping_transformation_DAG',
    default_args=default_args,
    description='A DAG for assignment x users table transformation',
    schedule_interval='0 23 * * *',
    catchup=False
)


def transform_data_per_query(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        sql='''select
            distinct 
            cast(concat(courses_courseusermapping.user_id,assignments_assignment.id,assignments_assignmentcourseuserquestionmapping.assignment_question_id) as double precision) as table_unique_key,
            courses_courseusermapping.user_id,
            assignments_assignment.id as assignment_id,
            assignments_assignmentcourseuserquestionmapping.assignment_question_id as question_id,
            cast(assignments_assignmentcourseuserquestionmapping.started_at as varchar) as question_started_at,
            cast(assignments_assignmentcourseuserquestionmapping.completed_at as varchar) as question_completed_at,
            assignments_assignmentcourseuserquestionmapping.completed,
            assignments_assignmentcourseuserquestionmapping.all_test_case_passed,
            case
            when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then 'coding'
            when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then 'frontend'
            when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then 'game'
            when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then 'project'
            when assignments_assignmentcourseuserquestionmapping.subjective_id is not null then 'subjective' else 'other' end as playground_type,
            case
            when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then assignments_assignmentcourseuserquestionmapping.coding_playground_id
            when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then assignments_assignmentcourseuserquestionmapping.front_end_playground_id
            when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then assignments_assignmentcourseuserquestionmapping.game_playground_id
            when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then assignments_assignmentcourseuserquestionmapping.project_playground_id
            when assignments_assignmentcourseuserquestionmapping.subjective_id is not null then assignments_assignmentcourseuserquestionmapping.subjective_id else null end as playground_id,
            
            assignments_assignmentcourseuserquestionmapping.hash,
            assignments_assignmentcourseuserquestionmapping.latest_assignment_question_hint_mapping_id,
            assignments_assignmentcourseuserquestionmapping.late_submission,
            assignments_assignmentcourseuserquestionmapping.max_test_case_passed,
            cast(assignments_assignmentcourseusermapping.started_at as varchar) as assignment_started_at,
            cast(assignments_assignmentcourseusermapping.completed_at as varchar) as assignment_completed_at,
            cast(assignments_assignmentcourseusermapping.cheated_marked_at as varchar) as assignment_cheated_marked_at,
            assignments_assignmentcourseusermapping.cheated,
            case
            when pcps.id is not null then (plag_coding.plagiarism_report #>> '{plagiarism_submission_id}')::float
            when pfps.id is not null then (plag_frontend.plagiarism_report #>> '{plagiarism_submission_id}')::float
            when ppps.id is not null then (plag_project.plagiarism_report #>> '{plagiarism_submission_id}')::float
            when pgps.id is not null then (plag_game.plagiarism_report #>> '{plagiarism_submission_id}')::float end as plagiarism_submission_id,
            case
            when pcps.id is not null then (plag_coding.plagiarism_report #>> '{plagiarism_score}')::float
            when pfps.id is not null then (plag_frontend.plagiarism_report #>> '{plagiarism_score}')::float
            when ppps.id is not null then (plag_project.plagiarism_report #>> '{plagiarism_score}')::float
            when pgps.id is not null then (plag_game.plagiarism_report #>> '{plagiarism_score}')::float end as plagiarism_score,
            case
            when pcps.id is not null then (plag_coding.plagiarism_report #>> '{solution_length}')::float
            when pfps.id is not null then (plag_frontend.plagiarism_report #>> '{solution_length}')::float
            when ppps.id is not null then (plag_project.plagiarism_report #>> '{solution_length}')::float
            when pgps.id is not null then (plag_game.plagiarism_report #>> '{solution_length}')::float end as solution_length,
            case
            when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then count(distinct pcps.id)
            when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then count(distinct pfps.id)
            when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then count(distinct pgps.id)
            when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then count(distinct ppps.id) else null end as number_of_submissions,
            case
            when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then count(distinct pcps.id) filter (where pcps.current_status not in (3))
            when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then count(distinct pfps.id) filter (where pfps.build_status not in (3))
            when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then count(distinct ppps.id) filter (where pfps.build_status not in (3)) else null end as error_faced_count
            
            from assignments_assignment
                left join courses_course 
                        on assignments_assignment.course_id = courses_course.id and (assignments_assignment.id between %d and %d)
                left join courses_courseusermapping 
                    on courses_courseusermapping.course_id = courses_course.id
                
                left join assignments_assignmentcourseusermapping 
                    on assignments_assignmentcourseusermapping.course_user_mapping_id = courses_courseusermapping.id 
                        and assignments_assignmentcourseusermapping.assignment_id = assignments_assignment.id
              
                left join assignments_assignmentcourseuserquestionmapping 
                    on assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id = assignments_assignmentcourseusermapping.id 
                        
                left join playgrounds_codingplaygroundsubmission pcps on pcps.coding_playground_id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
                left join playgrounds_playgroundplagiarismreport as plag_coding on plag_coding.object_id = pcps.id and plag_coding.content_type_id = 70
                
                left join playgrounds_frontendplaygroundsubmission pfps on pfps.front_end_playground_id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
                left join playgrounds_playgroundplagiarismreport as plag_frontend on plag_frontend.object_id = pfps.id and plag_frontend.content_type_id = 160
                
                left join playgrounds_projectplaygroundsubmission ppps on ppps.project_playground_id = assignments_assignmentcourseuserquestionmapping.project_playground_id
                left join playgrounds_playgroundplagiarismreport as plag_project on plag_project.object_id = ppps.id and plag_project.content_type_id = 165
                
                left join playgrounds_gameplaygroundsubmission pgps on pgps.game_playground_id = assignments_assignmentcourseuserquestionmapping.game_playground_id
                left join playgrounds_playgroundplagiarismreport as plag_game on plag_game.object_id = pgps.id and plag_game.content_type_id = 179
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,assignments_assignmentcourseuserquestionmapping.coding_playground_id,assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id
        ;
            ''' % (start_assignment_id, end_assignment_id),
    )


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_question_user_mapping (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY, 
            user_id bigint,
            assignment_id bigint,
            question_id bigint,
            question_started_at timestamp,
            question_completed_at timestamp,
            completed boolean,
            all_test_case_passed boolean,
            playground_type varchar(20),
            playground_id bigint,
            hash varchar(30),
            latest_assignment_question_hint_mapping_id bigint,
            late_submission boolean,
            max_test_case_passed int,
            assignment_started_at timestamp,
            assignment_completed_at timestamp,
            assignment_cheated_marked_at timestamp,
            cheated boolean,
            plagiarism_submission_id bigint,
            plagiarism_score double precision,
            solution_length bigint,
            number_of_submissions int,
            error_faced_count int
        );
    ''',
    dag=dag
)
for i in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{i}", dag=dag) as sub_dag_task_group:
        transform_data = transform_data_per_query(i * int(assignment_per_dags) + 1, (i + 1) * int(assignment_per_dags))
        extract_python_data = PythonOperator(
            task_id='extract_python_data',
            python_callable=extract_data_to_nested,
            provide_context=True,
            op_kwargs={'current_task_index': i},
            dag=dag,
        )
        transform_data >> extract_python_data
    create_table >> sub_dag_task_group
# sub_dag_index = Variable.get("sub_dag_index", default_var="0")
# sub_dag_index = int(sub_dag_index)
#
# with TaskGroup(group_id=f"transforming_data_{sub_dag_index}", dag=dag) as sub_dag_task_group:
#     transform_data = transform_data_per_query(sub_dag_index * int(assignment_per_dags) + 1, (sub_dag_index + 1) * int(assignment_per_dags))
#     extract_python_data = PythonOperator(
#         task_id='extract_python_data',
#         python_callable=extract_data_to_nested,
#         provide_context=True,
#         op_kwargs={'current_task_index': sub_dag_index},
#         dag=dag,
#     )
#     transform_data >> extract_python_data
#
# create_table >> sub_dag_task_group