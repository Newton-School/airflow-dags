from airflow import DAG
# from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

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
            'INSERT INTO arl_group_sessions_x_users (table_unique_key,'
            'meeting_id,'
            'session_name,'
            'session_date,'
            'mentor_user_id,'
            'mentor_name,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'mentee_user_id,'
            'mentee_course_user_mapping_id,'
            'mentee_name,'
            'lead_type,'
            'activity_status_7_days,'
            'activity_status_14_days,'
            'activity_status_30_days,'
            'student_category,'
            'label_mapping_status,'
            'enrolled_students,'
            'label_marked_students,'
            'isa_cancelled,'
            'deferred_students,'
            'foreclosed_students,'
            'rejected_by_ns_ops,'
            'mentor_total_time_in_seconds,'
            'mentor_total_time_in_mintues,'
            'total_overlap_time_in_seconds,'
            'total_overlap_time_in_mintues,'
            'user_placement_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set session_name = EXCLUDED.session_name,'
            'session_date = EXCLUDED.session_date,'
            'mentor_name = EXCLUDED.mentor_name,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'mentee_name = EXCLUDED.mentee_name,'
            'lead_type = EXCLUDED.lead_type,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'student_category = EXCLUDED.student_category,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'enrolled_students = EXCLUDED.enrolled_students,'
            'label_marked_students = EXCLUDED.label_marked_students,'
            'isa_cancelled = EXCLUDED.isa_cancelled,'
            'deferred_students = EXCLUDED.deferred_students,'
            'foreclosed_students = EXCLUDED.foreclosed_students,'
            'rejected_by_ns_ops = EXCLUDED.rejected_by_ns_ops,'
            'mentor_total_time_in_seconds = EXCLUDED.mentor_total_time_in_seconds,'
            'mentor_total_time_in_mintues = EXCLUDED.mentor_total_time_in_mintues,'
            'total_overlap_time_in_seconds = EXCLUDED.total_overlap_time_in_seconds,'
            'total_overlap_time_in_mintues = EXCLUDED.total_overlap_time_in_mintues,'
            'user_placement_status = EXCLUDED.user_placement_status;',
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
                transform_row[24],
                transform_row[25],
                transform_row[26],
                transform_row[27],
                transform_row[28],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_group_sessions_users_DAG',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for Group sessions x user for whom meetings were scheduled',
    schedule_interval='30 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_group_sessions_x_users (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            meeting_id bigint,
            session_name text,
            session_date date, 
            mentor_user_id bigint, 
            mentor_name text,
            course_id int, 
            course_name text, 
            course_structure_class text, 
            mentee_user_id bigint, 
            mentee_course_user_mapping_id bigint,
            mentee_name text, 
            lead_type text, 
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            student_category text,
            label_mapping_status text,
            enrolled_students int, 
            label_marked_students int,
            isa_cancelled int,
            deferred_students int,
            foreclosed_students int,
            rejected_by_ns_ops int,
            mentor_total_time_in_seconds real, 
            mentor_total_time_in_mintues real,
            total_overlap_time_in_seconds real,
            total_overlap_time_in_mintues real,
            user_placement_status text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        with gscur as 
            (select 
                meeting_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                user_type,
                extract('epoch' from (leave_time - join_time)) as total_time,
                overlapping_time_seconds,
                overlapping_time_minutes
            from
                group_session_course_user_reports
            group by 1,2,3,4,5,6,7,8),
            
        mentor_data as
            (select
                meeting_id,
                course_user_mapping_id,
                sum(total_time) as total_time_in_seconds
            from
                gscur
            where user_type like 'Mentor'
            group by 1,2),
            
        
        batch_strength_details as 
            (select 
                c.course_id,
                c.course_structure_id,
                count(distinct cum.user_id) filter (where cum.label_id is null and cum.status in (8,9)) as enrolled_students,
                count(distinct cum.user_id) filter (where cum.label_id is not null and cum.status in (8,9)) as label_marked_students,
                count(distinct cum.user_id) filter (where c.course_structure_id in (1,18) and cum.status in (11,12)) as isa_cancelled,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (30)) as deferred_students,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (11)) as foreclosed_students,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (12)) as rejected_by_ns_ops
            from
                courses c 
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
                    and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
            group by 1,2)
        
        select 
            concat(gs.meeting_id, gs.booked_by_id, gs.mentee_user_id) as table_unique_key,
            gs.meeting_id,
            gs.title as session_name,
            date(gs.start_timestamp) as session_date,
            gs.booked_by_id as mentor_user_id,
            concat(ui2.first_name,' ', ui2.last_name) as mentor_name,
            c.course_id,
            c.course_name,
            c.course_structure_class,
            gs.mentee_user_id,
            cum.course_user_mapping_id as mentee_course_user_mapping_id,
            concat(ui.first_name,' ', ui.last_name) as mentee_name,
            ui.lead_type,
            uasm.activity_status_7_days,
            uasm.activity_status_14_days,
            uasm.activity_status_30_days,
            cucm.student_category,
            case 
                when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            enrolled_students,
            label_marked_students,
            isa_cancelled,
            deferred_students,
            foreclosed_students,
            rejected_by_ns_ops,
            total_time_in_seconds as mentor_total_time_in_seconds,
            total_time_in_seconds / 60 as mentor_total_time_in_mintues,
            sum(gscur.overlapping_time_seconds) as total_overlap_time_in_seconds,
            sum(gscur.overlapping_time_minutes) as total_overlap_time_in_mintues,
            cum.user_placement_status
        from
            group_sessions gs
        join courses c 
            on c.course_id = gs.course_id and gs.with_mentees = true
                and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34) 
        join course_user_mapping cum 
            on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
                and gs.mentee_user_id = cum.user_id
        left join users_info ui 
            on ui.user_id = gs.mentee_user_id 
        left join users_info ui2 
            on ui2.user_id = gs.booked_by_id
        left join gscur 
            on gs.meeting_id = gscur.meeting_id 
                and cum.course_user_mapping_id = gscur.course_user_mapping_id
        left join user_activity_status_mapping uasm 
            on uasm.user_id = gs.mentee_user_id
        left join course_user_category_mapping cucm 
            on cucm.user_id = gs.mentee_user_id and cucm.course_id = gs.course_id
        left join batch_strength_details
            on batch_strength_details.course_id = c.course_id 
        left join mentor_data
            on mentor_data.meeting_id = gs.meeting_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,29
        order by 4 desc, 2, 5, 10;
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