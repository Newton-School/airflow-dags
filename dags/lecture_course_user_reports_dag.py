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
            'INSERT INTO lecture_course_user_reports (table_unique_key, user_id, student_name,'
            'lead_type, student_category, course_user_mapping_id, label_mapping_status,'
            'course_id, course_name, course_structure_class, lecture_id, lecture_title,'
            'mandatory, topic_template_id, template_name, inst_total_time_in_mins, inst_user_id,'
            'lecture_date, live_attendance, recorded_attendance,'
            'overall_attendance, total_overlapping_time_in_mins)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'student_category = EXCLUDED.student_category,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'lecture_title = EXCLUDED.lecture_title,'
            'mandatory = EXCLUDED.mandatory,'
            'template_name = EXCLUDED.template_name,'
            'inst_total_time_in_mins = EXCLUDED.inst_total_time_in_mins,'
            'inst_user_id = EXCLUDED.inst_user_id,'
            'lecture_date = EXCLUDED.lecture_date,'
            'live_attendance = EXCLUDED.live_attendance,'
            'recorded_attendance = EXCLUDED.recorded_attendance,'
            'overall_attendance = EXCLUDED.overall_attendance,'
            'total_overlapping_time_in_mins = EXCLUDED.total_overlapping_time_in_mins;',
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
    'lecture_course_user_reports_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Table at user, lecture level for all users with cum.status in (8,9,11,12,30)',
    schedule_interval='45 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_course_user_reports (
            id serial,
            table_unique_key varchar(255) not null PRIMARY KEY,
            user_id bigint,
            student_name varchar(255),
            lead_type varchar(255),
            student_category varchar(255),
            course_user_mapping_id bigint,
            label_mapping_status varchar(255),
            course_id int,
            course_name varchar(255),
            course_structure_class varchar(255),
            lecture_id bigint,
            lecture_title varchar(1028)
            mandatory boolean,
            topic_template_id int,
            template_name varchar(255),
            inst_total_time_in_mins int,
            inst_user_id bigint,
            lecture_date DATE,
            live_attendance int,
            recorded_attendance int,
            overall_attendance int,
            total_overlapping_time_in_mins int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with user_raw_data as
            (select
                lecture_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                user_type,
                overlapping_time_seconds,
                overlapping_time_minutes
            from
                lecture_engagement_time let
            where lower(user_type) like 'user'
            group by 1,2,3,4,5,6,7),
        
        inst_raw_data as
            (select 
                lecture_id,
                user_id as inst_user_id, 
                let.course_user_mapping_id as inst_cum_id,
                join_time,
                leave_time,
                extract('epoch' from (leave_time - join_time))/60 as time_diff_in_mins
            from
                lecture_engagement_time let
            join course_user_mapping cum 
                on cum.course_user_mapping_id = let.course_user_mapping_id 
            where lower(user_type) like 'instructor'
            group by 1,2,3,4,5,6),
            
        inst_data as 
            (select 
                lecture_id,
                inst_user_id,
                inst_cum_id,
                min(join_time) as inst_min_join_time,
                max(leave_time) as inst_max_join_time,
                sum(time_diff_in_mins) as inst_total_time_in_mins
            from
                inst_raw_data
            group by 1,2,3)
        select
            concat(cum.user_id, l.lecture_id, t.topic_template_id) as table_unique_key,
            cum.user_id,
            concat(ui.first_name,' ',ui.last_name) as student_name,
            ui.lead_type,
            cucm.student_category,
            cum.course_user_mapping_id,
            case 
                when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            c.course_id,
            c.course_name,
            c.course_structure_class,
            l.lecture_id,
            l.lecture_title,
            l.mandatory,
            t.topic_template_id,
            t.template_name,
            cast(inst_total_time_in_mins as int) as inst_total_time_in_mins,
            inst_user_id,
            date(l.start_timestamp) as lecture_date,
            count(distinct let.lecture_id) filter (where let.lecture_id is not null) as live_attendance,
            count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) as recorded_attendance,
            case
                when (count(distinct let.lecture_id) filter (where let.lecture_id is not null) = 1 or 
                    count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) = 1) then 1
                else 0 
            end as overall_attendance,
            cast(sum(let.overlapping_time_minutes) as int) as total_overlapping_time_in_mins
        from
            courses c
        join course_user_mapping cum
            on cum.course_id = c.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26)
                and cum.status in (8,9,11,12,30) and c.course_id in (select distinct wab.lu_course_id from wow_active_batches wab) 
        join lectures l
            on l.course_id = c.course_id and l.start_timestamp >= '2022-07-01'
        left join user_raw_data let
            on let.lecture_id = l.lecture_id and let.course_user_mapping_id = cum.course_user_mapping_id
        left join recorded_lectures_course_user_reports rlcur
            on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
        left join inst_data
            on inst_data.lecture_id = l.lecture_id
        left join lecture_topic_mapping ltm
            on ltm.lecture_id = l.lecture_id and ltm.completed = true
        left join topics t
            on t.topic_id = ltm.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
        left join users_info ui
            on ui.user_id = cum.user_id
        left join course_user_category_mapping cucm
            on cucm.user_id = cum.user_id and cucm.course_id = cum.course_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;
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