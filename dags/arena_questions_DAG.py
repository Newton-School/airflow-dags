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
                'INSERT INTO arena_questions_user_mapping (table_unique_key, user_id, assignment_question_id,'
                'module_name, started_at, completed_at,'
                'max_test_case_passed, completed, all_test_case_passed,'
                'playground_type, max_plag_score)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set module_name = EXCLUDED.module_name,'
                'started_at = EXCLUDED.started_at,'
                'completed_at = EXCLUDED.completed_at,'
                'max_test_case_passed = EXCLUDED.max_test_case_passed,'
                'completed = EXCLUDED.completed,'
                'all_test_case_passed = EXCLUDED.all_test_case_passed,'
                'playground_type = EXCLUDED.playground_type,'
                'max_plag_score = EXCLUDED.max_plag_score;',
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
                )
        )
    pg_conn.commit()


dag = DAG(
    'arena_question_user_mapping_dag',
    default_args=default_args,
    description='Arena questions cross users details',
    schedule_interval='30 19 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arena_questions_user_mapping (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            assignment_question_id bigint,
            module_name text,
            started_at timestamp, 
            completed_at timestamp,
            max_test_case_passed int,
            completed boolean,
            all_test_case_passed boolean,
            playground_type text,
            max_plag_score real
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
        uk as table_unique_key, 
        user_id,
        assignment_question_id,
        module_name,
        started_at,
        completed_at,
        max_test_case_passed,
        completed,
        all_test_case_passed, 
        playground_type,
        max(plagiarism_score) as max_plag_score
    from
        (select
            concat(auth_user.id, assignments_milestoneuserquestionmapping.assignment_question_id, auth_user.id) as uk,
            auth_user.id as user_id,
            assignments_milestoneuserquestionmapping.assignment_question_id,
            technologies_topictemplate.title as module_name,
            assignments_milestoneuserquestionmapping.started_at,
            assignments_milestoneuserquestionmapping.completed_at,
            assignments_milestoneuserquestionmapping.max_test_case_passed,
            assignments_milestoneuserquestionmapping.completed,
            assignments_milestoneuserquestionmapping.all_test_case_passed,
            
            case
                when assignments_milestoneuserquestionmapping.coding_playground_id is not null then 'coding'
                when assignments_milestoneuserquestionmapping.front_end_playground_id is not null then 'frontend'
                when assignments_milestoneuserquestionmapping.game_playground_id is not null then 'game'
                when assignments_milestoneuserquestionmapping.project_playground_id is not null then 'project'
                when assignments_milestoneuserquestionmapping.subjective_id is not null then 'subjective' else 'other' 
            end as playground_type,
            
            case
                when pcps.id is not null then (plag_coding.plagiarism_report #>> '{plagiarism_score}')::float
                when pfps.id is not null then (plag_frontend.plagiarism_report #>> '{plagiarism_score}')::float
                when ppps.id is not null then (plag_project.plagiarism_report #>> '{plagiarism_score}')::float
                when pgps.id is not null then (plag_game.plagiarism_report #>> '{plagiarism_score}')::float 
            end as plagiarism_score
        from
            assignments_milestoneuserquestionmapping
            
        join auth_user
            on auth_user.id = assignments_milestoneuserquestionmapping.user_id
            
        left join assignments_assignmentquestion
            on assignments_assignmentquestion.id = assignments_milestoneuserquestionmapping.assignment_question_id
            
        left join playgrounds_codingplaygroundsubmission pcps 
            on pcps.coding_playground_id = assignments_milestoneuserquestionmapping.coding_playground_id
            
        left join playgrounds_playgroundplagiarismreport as plag_coding 
            on plag_coding.object_id = pcps.id and plag_coding.content_type_id = 70
        
        left join playgrounds_frontendplaygroundsubmission pfps 
            on pfps.front_end_playground_id = assignments_milestoneuserquestionmapping.front_end_playground_id
            
        left join playgrounds_playgroundplagiarismreport as plag_frontend 
            on plag_frontend.object_id = pfps.id and plag_frontend.content_type_id = 160
        
        left join playgrounds_projectplaygroundsubmission ppps 
            on ppps.project_playground_id = assignments_milestoneuserquestionmapping.project_playground_id
            
        left join playgrounds_playgroundplagiarismreport as plag_project 
            on plag_project.object_id = ppps.id and plag_project.content_type_id = 165
        
        left join playgrounds_gameplaygroundsubmission pgps 
            on pgps.game_playground_id = assignments_milestoneuserquestionmapping.game_playground_id
            
        left join playgrounds_playgroundplagiarismreport as plag_game
            on plag_game.object_id = pgps.id and plag_game.content_type_id = 179
        
        join assignments_assignmentquestiontopicmapping
            on assignments_assignmentquestiontopicmapping.assignment_question_id = assignments_milestoneuserquestionmapping.assignment_question_id 
                and assignments_assignmentquestiontopicmapping.main_topic = true
    
        join technologies_topic
            on technologies_topic.id = assignments_assignmentquestiontopicmapping.topic_id
        
        join technologies_topicnode
            on technologies_topicnode.topic_id = technologies_topic.id
        
        join technologies_topictemplate
            on technologies_topictemplate.id = technologies_topicnode.topic_template_id and technologies_topictemplate.id in (102,103,119,334,336,338,339,340,341,342,344,410)
    
        where assignments_milestoneuserquestionmapping.content_type_id is null
        group by 1,2,3,4,5,6,7,8,9,10,11) a 
        group by 1,2,3,4,5,6,7,8,9,10;
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