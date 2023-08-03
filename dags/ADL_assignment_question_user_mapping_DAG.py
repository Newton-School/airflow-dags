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

assignment_per_dags = Variable.get("assignment_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'NEW_Assignment_question_user_mapping_DAG',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Assignment Question User Mapping Table DAG with latest submission and max plagiarism value',
    schedule_interval='0 21 * * SUN',
    catchup=False
)

# Root Level Create Table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_question_user_mapping_new (
            id serial not null,
            table_unique_key text not null PRIMARY KEY, 
            user_id bigint,
            assignment_id bigint,
            question_id bigint,
            question_started_at timestamp,
            question_completed_at timestamp,
            completed boolean,
            all_test_case_passed boolean,
            playground_type text,
            playground_id bigint,
            playground_hash text,
            hash text,
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


# Leaf Level Abstraction
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_assignment_sub_dag_id = kwargs['current_assignment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(
            'INSERT INTO assignment_question_user_mapping (table_unique_key, user_id, assignment_id, '
            'question_id, question_started_at,'
            'question_completed_at, completed, all_test_case_passed,'
            'playground_type, playground_id, playground_hash, hash,'
            'latest_assignment_question_hint_mapping_id, late_submission,'
            'max_test_case_passed, assignment_started_at,'
            'assignment_completed_at, assignment_cheated_marked_at, cheated,'
            'plagiarism_submission_id, plagiarism_score, solution_length, '
            'number_of_submissions, error_faced_count)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set question_started_at=EXCLUDED.question_started_at,'
            'question_completed_at = EXCLUDED.question_completed_at,'
            'completed=EXCLUDED.completed, all_test_case_passed=EXCLUDED.all_test_case_passed, '
            'playground_type = EXCLUDED.playground_type, '
            'playground_id = EXCLUDED.playground_id,'
            'playground_hash=EXCLUDED.playground_hash,'
            'latest_assignment_question_hint_mapping_id=EXCLUDED.latest_assignment_question_hint_mapping_id,'
            'late_submission=EXCLUDED.late_submission,'
            'max_test_case_passed=EXCLUDED.max_test_case_passed,'
            'assignment_started_at=EXCLUDED.assignment_started_at,'
            'assignment_completed_at=EXCLUDED.assignment_completed_at,'
            'assignment_cheated_marked_at=EXCLUDED.assignment_cheated_marked_at, '
            'cheated=EXCLUDED.cheated,'
            'plagiarism_submission_id=EXCLUDED.plagiarism_submission_id,'
            'plagiarism_score=EXCLUDED.plagiarism_score,'
            'solution_length=EXCLUDED.solution_length, '
            'number_of_submissions=EXCLUDED.number_of_submissions,'
            'error_faced_count=EXCLUDED.error_faced_count;',
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
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_assignment_sub_dag_func(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='number_of_rows_per_assignment_sub_dag',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        sql=''' select count(table_unique_key) from
        (with latest_submission as(
                select
                
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
                
                case
                when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then max(pcps.id)
                when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then max(pfps.id)
                when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then max(pgps.id)
                when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then max(ppps.id) else null end as playground_submission_id
                
                from assignments_assignmentcourseuserquestionmapping
                left join playgrounds_codingplayground pcp on pcp.id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
                left join playgrounds_frontendplayground pfp on pfp.id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
                left join playgrounds_gameplayground pgp on pgp.id = assignments_assignmentcourseuserquestionmapping.game_playground_id
                left join playgrounds_projectplayground ppp on ppp.id = assignments_assignmentcourseuserquestionmapping.project_playground_id
                left join playgrounds_codingplaygroundsubmission pcps on pcps.coding_playground_id = pcp.id
                left join playgrounds_frontendplaygroundsubmission pfps on pfps.front_end_playground_id = pfp.id
                left join playgrounds_projectplaygroundsubmission ppps on ppps.project_playground_id = ppp.id
                left join playgrounds_gameplaygroundsubmission pgps on pgps.game_playground_id = pgp.id
                
                group by 1,2,assignments_assignmentcourseuserquestionmapping.coding_playground_id,assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id
                order by 1,2
                )
                select
                        distinct 
                        concat(courses_courseusermapping.user_id,assignments_assignmentcourseuserquestionmapping.assignment_question_id,assignments_assignment.id,assignments_assignmentcourseusermapping.id) as table_unique_key,
                        courses_courseusermapping.user_id,
                        assignments_assignment.id as assignment_id,
                        assignments_assignmentcourseuserquestionmapping.assignment_question_id as question_id,
                        assignments_assignmentcourseuserquestionmapping.started_at as question_started_at,
                        assignments_assignmentcourseuserquestionmapping.completed_at as question_completed_at,
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
                        
                        case
                        when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then pcp.hash
                        when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then pfp.hash
                        when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then pgp.hash
                        when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then ppp.hash
                        else null end as playground_hash,
                        
                        assignments_assignmentcourseuserquestionmapping.hash,
                        assignments_assignmentcourseuserquestionmapping.latest_assignment_question_hint_mapping_id,
                        assignments_assignmentcourseuserquestionmapping.late_submission,
                        assignments_assignmentcourseuserquestionmapping.max_test_case_passed,
                        assignments_assignmentcourseusermapping.started_at as assignment_started_at,
                        assignments_assignmentcourseusermapping.completed_at as assignment_completed_at,
                        assignments_assignmentcourseusermapping.cheated_marked_at as assignment_cheated_marked_at,
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
                            
                            join assignments_assignmentcourseusermapping 
                                on assignments_assignmentcourseusermapping.course_user_mapping_id = courses_courseusermapping.id 
                                    and assignments_assignmentcourseusermapping.assignment_id = assignments_assignment.id
                          
                            left join assignments_assignmentcourseuserquestionmapping 
                                on assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id = assignments_assignmentcourseusermapping.id 
                                
                            left join playgrounds_codingplayground pcp on pcp.id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
                            left join playgrounds_frontendplayground pfp on pfp.id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
                            left join playgrounds_gameplayground pgp on pgp.id = assignments_assignmentcourseuserquestionmapping.game_playground_id
                            left join playgrounds_projectplayground ppp on ppp.id = assignments_assignmentcourseuserquestionmapping.project_playground_id
                            
                            left join latest_submission as latest_submission_coding on latest_submission_coding.playground_id = assignments_assignmentcourseuserquestionmapping.coding_playground_id and latest_submission_coding.playground_type = 'coding'
                            left join latest_submission as latest_submission_frontend on latest_submission_frontend.playground_id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id and latest_submission_frontend.playground_type = 'frontend'
                            left join latest_submission as latest_submission_game on latest_submission_game.playground_id = assignments_assignmentcourseuserquestionmapping.game_playground_id and latest_submission_game.playground_type = 'game'
                            left join latest_submission as latest_submission_project on latest_submission_project.playground_id = assignments_assignmentcourseuserquestionmapping.project_playground_id and latest_submission_project.playground_type = 'project'
                            
                                    
                            left join playgrounds_codingplaygroundsubmission pcps on pcps.id = latest_submission_coding.playground_submission_id
                            left join playgrounds_playgroundplagiarismreport as plag_coding on plag_coding.object_id = pcps.id and plag_coding.content_type_id = 70
                            
                            left join playgrounds_frontendplaygroundsubmission pfps on pfps.id = latest_submission_frontend.playground_submission_id
                            left join playgrounds_playgroundplagiarismreport as plag_frontend on plag_frontend.object_id = pfps.id and plag_frontend.content_type_id = 160
                            
                            left join playgrounds_projectplaygroundsubmission ppps on ppps.id = latest_submission_project.playground_submission_id
                            left join playgrounds_playgroundplagiarismreport as plag_project on plag_project.object_id = ppps.id and plag_project.content_type_id = 165
                            
                            left join playgrounds_gameplaygroundsubmission pgps on pgps.id = latest_submission_game.playground_submission_id
                            left join playgrounds_playgroundplagiarismreport as plag_game on plag_game.object_id = pgps.id and plag_game.content_type_id = 179
                        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,assignments_assignmentcourseuserquestionmapping.coding_playground_id,
                        assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id) query_rows;
            ''' % (start_assignment_id, end_assignment_id),
    )


# Python Limit Offset generator
def limit_offset_generator_func(**kwargs):
    ti = kwargs['ti']
    current_assignment_sub_dag_id = kwargs['current_assignment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    count_cps_rows = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assignment_sub_dag_id}.number_of_rows_per_assignment_sub_dag')
    print(count_cps_rows)
    total_count_rows = count_cps_rows[0][0]
    return {
        "limit": total_count_rows // total_number_of_extraction_cps_dags,
        "offset": current_cps_sub_dag_id * (total_count_rows // total_number_of_extraction_cps_dags) + 1,
    }


# TODO: Add Count Logic
def transform_data_per_query(start_assignment_id, end_assignment_id, cps_sub_dag_id, current_assignment_sub_dag_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_assignment_sub_dag_id': current_assignment_sub_dag_id,
            'task_key': f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql=''' select * from
        (with latest_submission as(
select

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

case
when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then max(pcps.id)
when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then max(pfps.id)
when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then max(pgps.id)
when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then max(ppps.id) else null end as playground_submission_id

from assignments_assignmentcourseuserquestionmapping
left join playgrounds_codingplayground pcp on pcp.id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
left join playgrounds_frontendplayground pfp on pfp.id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
left join playgrounds_gameplayground pgp on pgp.id = assignments_assignmentcourseuserquestionmapping.game_playground_id
left join playgrounds_projectplayground ppp on ppp.id = assignments_assignmentcourseuserquestionmapping.project_playground_id
left join playgrounds_codingplaygroundsubmission pcps on pcps.coding_playground_id = pcp.id
left join playgrounds_frontendplaygroundsubmission pfps on pfps.front_end_playground_id = pfp.id
left join playgrounds_projectplaygroundsubmission ppps on ppps.project_playground_id = ppp.id
left join playgrounds_gameplaygroundsubmission pgps on pgps.game_playground_id = pgp.id

group by 1,2,assignments_assignmentcourseuserquestionmapping.coding_playground_id,assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id
order by 1,2
)
select
            distinct 
            concat(courses_courseusermapping.user_id,assignments_assignmentcourseuserquestionmapping.assignment_question_id,assignments_assignment.id,assignments_assignmentcourseusermapping.id) as table_unique_key,
            courses_courseusermapping.user_id,
            assignments_assignment.id as assignment_id,
            assignments_assignmentcourseuserquestionmapping.assignment_question_id as question_id,
            assignments_assignmentcourseuserquestionmapping.started_at as question_started_at,
            assignments_assignmentcourseuserquestionmapping.completed_at as question_completed_at,
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
            
            case
            when assignments_assignmentcourseuserquestionmapping.coding_playground_id is not null then pcp.hash
            when assignments_assignmentcourseuserquestionmapping.front_end_playground_id is not null then pfp.hash
            when assignments_assignmentcourseuserquestionmapping.game_playground_id is not null then pgp.hash
            when assignments_assignmentcourseuserquestionmapping.project_playground_id is not null then ppp.hash
            else null end as playground_hash,
            
            assignments_assignmentcourseuserquestionmapping.hash,
            assignments_assignmentcourseuserquestionmapping.latest_assignment_question_hint_mapping_id,
            assignments_assignmentcourseuserquestionmapping.late_submission,
            assignments_assignmentcourseuserquestionmapping.max_test_case_passed,
            assignments_assignmentcourseusermapping.started_at as assignment_started_at,
            assignments_assignmentcourseusermapping.completed_at as assignment_completed_at,
            assignments_assignmentcourseusermapping.cheated_marked_at as assignment_cheated_marked_at,
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
                
                join assignments_assignmentcourseusermapping 
                    on assignments_assignmentcourseusermapping.course_user_mapping_id = courses_courseusermapping.id 
                        and assignments_assignmentcourseusermapping.assignment_id = assignments_assignment.id
              
                left join assignments_assignmentcourseuserquestionmapping 
                    on assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id = assignments_assignmentcourseusermapping.id 
                    
                left join playgrounds_codingplayground pcp on pcp.id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
                left join playgrounds_frontendplayground pfp on pfp.id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
                left join playgrounds_gameplayground pgp on pgp.id = assignments_assignmentcourseuserquestionmapping.game_playground_id
                left join playgrounds_projectplayground ppp on ppp.id = assignments_assignmentcourseuserquestionmapping.project_playground_id
                
                left join latest_submission as latest_submission_coding on latest_submission_coding.playground_id = assignments_assignmentcourseuserquestionmapping.coding_playground_id and latest_submission_coding.playground_type = 'coding'
                left join latest_submission as latest_submission_frontend on latest_submission_frontend.playground_id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id and latest_submission_frontend.playground_type = 'frontend'
                left join latest_submission as latest_submission_game on latest_submission_game.playground_id = assignments_assignmentcourseuserquestionmapping.game_playground_id and latest_submission_game.playground_type = 'game'
                left join latest_submission as latest_submission_project on latest_submission_project.playground_id = assignments_assignmentcourseuserquestionmapping.project_playground_id and latest_submission_project.playground_type = 'project'
                
                        
                left join playgrounds_codingplaygroundsubmission pcps on pcps.id = latest_submission_coding.playground_submission_id
                left join playgrounds_playgroundplagiarismreport as plag_coding on plag_coding.object_id = pcps.id and plag_coding.content_type_id = 70
                
                left join playgrounds_frontendplaygroundsubmission pfps on pfps.id = latest_submission_frontend.playground_submission_id
                left join playgrounds_playgroundplagiarismreport as plag_frontend on plag_frontend.object_id = pfps.id and plag_frontend.content_type_id = 160
                
                left join playgrounds_projectplaygroundsubmission ppps on ppps.id = latest_submission_project.playground_submission_id
                left join playgrounds_playgroundplagiarismreport as plag_project on plag_project.object_id = ppps.id and plag_project.content_type_id = 165
                
                left join playgrounds_gameplaygroundsubmission pgps on pgps.id = latest_submission_game.playground_submission_id
                left join playgrounds_playgroundplagiarismreport as plag_game on plag_game.object_id = pgps.id and plag_game.content_type_id = 179
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,assignments_assignmentcourseuserquestionmapping.coding_playground_id,
            assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id
        ) final_query
        limit {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').limit }} 
        offset {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').offset }}
        ;
            ''' % (start_assignment_id, end_assignment_id),
    )


for assignment_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{assignment_sub_dag_id}", dag=dag) as assignment_sub_dag_task_group:
        assignment_start_id = assignment_sub_dag_id * int(assignment_per_dags) + 1
        assignment_end_id = (assignment_sub_dag_id + 1) * int(assignment_per_dags)
        number_of_rows_per_assignment_sub_dag = number_of_rows_per_assignment_sub_dag_func(assignment_start_id,
                                                                                           assignment_end_id)

        for cps_sub_dag_id in range(int(total_number_of_extraction_cps_dags)):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_assignment_sub_dag_{assignment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag) as cps_sub_dag:
                limit_offset_generator = PythonOperator(
                    task_id='limit_offset_generator',
                    python_callable=limit_offset_generator_func,
                    provide_context=True,
                    op_kwargs={
                        'current_assignment_sub_dag_id': assignment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id,
                    },
                    dag=dag,
                )

                transform_data = transform_data_per_query(assignment_start_id, assignment_end_id, cps_sub_dag_id,
                                                          assignment_sub_dag_id)

                extract_python_data = PythonOperator(
                    task_id='extract_python_data',
                    python_callable=extract_data_to_nested,
                    provide_context=True,
                    op_kwargs={
                        'current_assignment_sub_dag_id': assignment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id
                    },
                    dag=dag,
                )

                limit_offset_generator >> transform_data >> extract_python_data

            number_of_rows_per_assignment_sub_dag >> cps_sub_dag

    create_table >> assignment_sub_dag_task_group