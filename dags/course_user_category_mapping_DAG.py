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
            'INSERT INTO course_user_category_mapping (table_unique_key, course_id, course_name,'
            'course_structure_class, user_id,'
            'course_user_mapping_status, label_mapping_status,'
            'completed_module_count, count_of_a, student_category)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'course_user_mapping_status = EXCLUDED.course_user_mapping_status,'
            'completed_module_count = EXCLUDED.completed_module_count,'
            'count_of_a = EXCLUDED.count_of_a,'
            'student_category = EXCLUDED.student_category;',
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
    'course_user_category_mapping_DAG',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='assigns category from (A1,A2,A3,B) to each user_id with cum.status in (8,9,11,12,30) per course_id',
    schedule_interval='30 2 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_user_category_mapping (
            id serial,
            table_unique_key varchar(255) not null PRIMARY KEY,
            course_id bigint,
            course_name varchar(255),
            course_structure_class varchar(255),
            user_id bigint,
            course_user_mapping_status int,
            label_mapping_status varchar(255),
            completed_module_count int,
            count_of_a int,
            student_category varchar(255)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        with user_level_data as 
            (with all_data as
                    (with batch_module_mapping as
                        (with module_raw as
                            (select
                                a.assignment_id,
                                a.title as contest_name,
                                a.course_id,
                                c.course_name,
                                date(a.start_timestamp) as contest_date,
                                t.topic_template_id,
                                t.template_name
                            from 
                                assignments a
                            join courses c 
                                on c.course_id = a.course_id and original_assignment_type in (3,4) 
                                    and assignment_sub_type = 4
                            join assignment_topic_mapping atm 
                                on atm.assignment_id = a.assignment_id
                            left join topics t 
                                on t.topic_id = atm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                            group by 1,2,3,4,5,6,7
                            order by 1 desc, 3, 4 desc)
                        
                        select distinct
                            course_id,	
                            course_name,
                            topic_template_id,
                            case 
                                when topic_template_id in (102,344) then 145 -- react
                                when topic_template_id in (103) then 146 -- JS
                                when topic_template_id in (119,342) then 150 -- HTML/CSS
                                when topic_template_id in (334) then 173 -- LDSA-1
                                when topic_template_id in (338) then 177 -- LDSA-2
                            end as rating_topic_template_id,
                            -- introduced to map NS_datascience DB topic_pool_id to prod topic_template_id
                            template_name
                        from
                            module_raw
                        where topic_template_id in (102,103,119,334,338,342,344)
                        group by 1,2,3,4,5
                        order by 1 desc, 3)
                    
                select 
                    aur.course_id,
                    aur.course_name,
                    aur.course_structure_class,
                    batch_module_mapping.topic_template_id,
                    batch_module_mapping.template_name as contest_module_name,
                    aur.user_id,
                    aur.course_user_mapping_status,
                    aur.label_mapping_status,
                    aur.topic_pool_id as rating_topic_template_id,
                    aur.template_name as rating_template_name,
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
                    arl_user_ratings aur
                left join batch_module_mapping
                    on aur.course_id = batch_module_mapping.course_id 
                        and batch_module_mapping.rating_topic_template_id = aur.topic_pool_id
                where aur.course_structure_id <> 32),
                
            completed_module_count as 
                (select
                    course_id,
                    course_name,
                    count(distinct rating_topic_template_id) filter (where all_data.topic_template_id is not null) as comp_module_count
                from
                    all_data
                group by 1,2)    
            select
                all_data.course_id,
                all_data.course_name,
                all_data.course_structure_class,
                all_data.user_id,
                all_data.course_user_mapping_status,
                all_data.label_mapping_status,
                case 
                    when all_data.course_id not in (select distinct course_id from completed_module_count where comp_module_count > 0) and all_data.course_id <= 400 then 5
                    else completed_module_count.comp_module_count
                end as completed_module_count,
                count(distinct rating_topic_template_id) filter (where grade_obtained like 'A') as count_of_a
            from
                all_data
            left join completed_module_count
                on completed_module_count.course_id = all_data.course_id
            group by 1,2,3,4,5,6,7
            order by 1 desc, 3)
        
        select 
            concat(course_id, user_id, course_id) as table_unique_key,
            user_level_data.*,
            case
                when completed_module_count <> 0 and (count_of_a * 1.0 / completed_module_count) >= 0.9 then 'A1'
                when completed_module_count <> 0 and (count_of_a * 1.0 / completed_module_count) >= 0.6 and (count_of_a * 1.0 / completed_module_count) < 0.9 then 'A2'
                when completed_module_count <> 0 and (count_of_a * 1.0 / completed_module_count) >= 0.3 and (count_of_a * 1.0 / completed_module_count) < 0.6 then 'A3'
                when completed_module_count <> 0 and (count_of_a * 1.0 / completed_module_count) < 0.3 then 'B'
                else null
            end as student_category
        from
            user_level_data;
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