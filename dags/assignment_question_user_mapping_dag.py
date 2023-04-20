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
                'INSERT INTO assignment_question_user_mapping (user_id,assignment_id,question_id,question_started_at,'
                'question_completed_at,completed,all_test_case_passed,playground_type,playground_id,hash,'
                'latest_assignment_question_hint_mapping_id,late_submission,max_test_case_passed,assignment_started_at,'
                'assignment_completed_at,assignment_cheated_marked_at,cheated,plagiarism_submission_id,'
                'plagiarism_score,solution_length,number_of_submissions,error_faced_count) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (unique_user_id_assignment_id_question_id) do update set question_completed_at = EXCLUDED.question_completed_at,'
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'Assignment_question_user_mapping_DAG',
    default_args=default_args,
    description='Assignment Question User Mapping Table DAG',
    schedule_interval='0 23 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_question_user_mapping (
            id serial not null PRIMARY KEY,
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
            error_faced_count int,
            CONSTRAINT unique_user_id_assignment_id_question_id UNIQUE (user_id,assignment_id,question_id)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with questions_released as(
                (select
                            distinct courses_courseusermapping.user_id,
                            courses_course.id as course_id,
                            assignments_assignment.id as assignment_id,
                            aaq.id as question_id
                        from 
                            assignments_assignment
                        join courses_course
                            on courses_course.id = assignments_assignment.course_id
                        left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
                        left join courses_coursestructure
                            on courses_coursestructure.id = courses_course.course_structure_id
                        join assignments_assignmentquestionmapping aaqm
                            on aaqm.assignment_id = assignments_assignment.id
                        join assignments_assignmentquestion aaq
                            on aaq.id = aaqm.assignment_question_id
                        where assignments_assignment.id between 15000 and 20000
                        order by 1,2)
                        
                        union 
                
                        (select
                                courses_courseusermapping.user_id,
                                courses_course.id as course_id,
                                assignments_assignment.id  as assignment_id,
                                assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_question_id as question_id
                            from
                                assignments_assignment
                            left join courses_course 
                                on courses_course.id = assignments_assignment.course_id
                            left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
                            left join courses_coursestructure
                                on courses_coursestructure.id = courses_course.course_structure_id
                            left join assignments_assignmentcourseuserrandomassignedquestionmapping 
                                on assignments_assignmentcourseuserrandomassignedquestionmapping.course_user_mapping_id = courses_courseusermapping.id
                                and assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_id = assignments_assignment.id
                            
                            where (assignments_assignment.id between 15000 and 20000) and assignments_assignment.original_assignment_type in (3,4))
                )
                select
                distinct questions_released.user_id,
                questions_released.assignment_id,
                questions_released.question_id,
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
                
                from questions_released
                    left join courses_courseusermapping 
                        on courses_courseusermapping.user_id = questions_released.user_id 
                            and courses_courseusermapping.course_id = questions_released.course_id
                    left join assignments_assignmentcourseusermapping 
                        on assignments_assignmentcourseusermapping.course_user_mapping_id = courses_courseusermapping.id 
                            and questions_released.assignment_id = assignments_assignmentcourseusermapping.assignment_id
                    left join assignments_assignmentcourseuserquestionmapping 
                        on assignments_assignmentcourseuserquestionmapping.assignment_course_user_mapping_id = assignments_assignmentcourseusermapping.id 
                            and assignments_assignmentcourseuserquestionmapping.assignment_question_id = questions_released.question_id
                    left join playgrounds_codingplaygroundsubmission pcps on pcps.coding_playground_id = assignments_assignmentcourseuserquestionmapping.coding_playground_id
                    left join playgrounds_playgroundplagiarismreport as plag_coding on plag_coding.object_id = pcps.id and plag_coding.content_type_id = 70
                    
                    left join playgrounds_frontendplaygroundsubmission pfps on pfps.front_end_playground_id = assignments_assignmentcourseuserquestionmapping.front_end_playground_id
                    left join playgrounds_playgroundplagiarismreport as plag_frontend on plag_frontend.object_id = pfps.id and plag_frontend.content_type_id = 160
                    
                    left join playgrounds_projectplaygroundsubmission ppps on ppps.project_playground_id = assignments_assignmentcourseuserquestionmapping.project_playground_id
                    left join playgrounds_playgroundplagiarismreport as plag_project on plag_project.object_id = ppps.id and plag_project.content_type_id = 165
                    
                    left join playgrounds_gameplaygroundsubmission pgps on pgps.game_playground_id = assignments_assignmentcourseuserquestionmapping.game_playground_id
                    left join playgrounds_playgroundplagiarismreport as plag_game on plag_game.object_id = pgps.id and plag_game.content_type_id = 179
                    where questions_released.assignment_id between 15000 and 20000
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,assignments_assignmentcourseuserquestionmapping.coding_playground_id,assignments_assignmentcourseuserquestionmapping.front_end_playground_id,assignments_assignmentcourseuserquestionmapping.game_playground_id,assignments_assignmentcourseuserquestionmapping.project_playground_id
    ;
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