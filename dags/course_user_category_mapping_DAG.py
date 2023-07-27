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
            'completed_module_count, count_of_a, student_category, student_name)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'course_user_mapping_status = EXCLUDED.course_user_mapping_status,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'completed_module_count = EXCLUDED.completed_module_count,'
            'count_of_a = EXCLUDED.count_of_a,'
            'student_category = EXCLUDED.student_category,'
            'student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type;',
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
    schedule_interval='15 1 * * *',
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
            student_category varchar(255),
            student_name text,
            lead_type text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
select
        concat(course_id, user_id, course_id) as table_unique_key,
        course_id,
        course_name,
        course_structure_class,
        user_id,
        course_user_mapping_status,
        label_mapping_status,
        comp_module_count as completed_module_count,
        count_of_a,
        case
            when comp_module_count <> 0 and (count_of_a * 1.0 / comp_module_count) >= 1 then 'A1'
            when comp_module_count <> 0 and (count_of_a * 1.0 / comp_module_count) >= 0.5 and (count_of_a * 1.0 / comp_module_count) < 1 then 'A2'
            when comp_module_count <> 0 and (count_of_a * 1.0 / comp_module_count) > 0 and (count_of_a * 1.0 / comp_module_count) < 0.5 then 'A3'
            when comp_module_count <> 0 and (count_of_a * 1.0 / comp_module_count) <= 0 then 'B'
            else null
        end as student_category,
        student_name,
        lead_type
    from
        (with batch_module_mapping as
            (select 
                id as table_uk,
                course_id,
                course_name,
                topic_pool_id,
                module_name,
                module_completion_status 
            from
                batch_module_completion_status bmcs 
            where module_completion_status = true),
        
        completed_module_count as 
            (select
                course_id,
                course_name,
                count(distinct topic_pool_id) filter (where module_completion_status is true) as comp_module_count
            from
                batch_module_completion_status bmcs
            group by 1,2),
        
        required_user_data as 
           (select 
                aur.course_id,
                aur.course_name,
                aur.course_structure_class,
                aur.user_id,
                aur.course_user_mapping_status,
                aur.label_mapping_status,
                aur.topic_pool_id,
                batch_module_mapping.table_uk,
                batch_module_mapping.module_completion_status,
                aur.template_name,
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
            join batch_module_mapping
                on batch_module_mapping.course_id = aur.course_id and aur.topic_pool_id = batch_module_mapping.topic_pool_id
            where aur.course_structure_id <> 32)
            
            
        select 
            c.course_id,
            c.course_name,
            c.course_structure_class,
            cum.user_id,
            concat(ui.first_name,' ',ui.last_name) as student_name,
            cum.status as course_user_mapping_status,
            case 
                when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            completed_module_count.comp_module_count,
            count(distinct topic_pool_id) filter (where grade_obtained like 'A') as count_of_a,
            ui.lead_type
        from
            course_user_mapping cum 
        join courses c 
            on c.course_id = cum.course_id and cum.status in (8,9,11,12,30)
        left join users_info ui
            	on ui.user_id = cum.user_id
        left join required_user_data
            on required_user_data.user_id = cum.user_id and cum.course_id = required_user_data.course_id
        left join completed_module_count
            on completed_module_count.course_id = c.course_id
        group by 1,2,3,4,5,6,7,8,10) final_query
    order by 2 desc, 5;
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