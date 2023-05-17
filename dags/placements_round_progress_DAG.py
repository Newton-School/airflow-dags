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
                'INSERT INTO placements_round_progress (company_course_user_mapping_progress_id,company_course_user_mapping_id,'
                'created_at,created_by_id,description,start_timestamp,end_timestamp,feedback,hash,'
                'questions_asked,reason_to_not_attending,rescheduled_at,status,student_evaluation,'
                'timestamp_added_at,title,round_type,url,will_student_attend,your_experience,did_student_attend,'
                'reason_not_attended,round_1,round_2,round_3,round_4,pre_final_round,final_round,no_show) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (company_course_user_mapping_progress_id) do update set '
                'description =EXCLUDED.description,start_timestamp =EXCLUDED.start_timestamp,end_timestamp =EXCLUDED.end_timestamp,'
                'feedback =EXCLUDED.feedback,hash =EXCLUDED.hash,questions_asked =EXCLUDED.questions_asked,'
                'reason_to_not_attending =EXCLUDED.reason_to_not_attending,rescheduled_at =EXCLUDED.rescheduled_at,'
                'status =EXCLUDED.status,student_evaluation =EXCLUDED.student_evaluation,timestamp_added_at =EXCLUDED.timestamp_added_at,'
                'title =EXCLUDED.title,round_type =EXCLUDED.round_type,url =EXCLUDED.url,will_student_attend =EXCLUDED.will_student_attend,'
                'your_experience =EXCLUDED.your_experience,did_student_attend =EXCLUDED.did_student_attend,reason_not_attended =EXCLUDED.reason_not_attended,'
                'round_1 =EXCLUDED.round_1,round_2 =EXCLUDED.round_2,round_3 =EXCLUDED.round_3,round_4 =EXCLUDED.round_4,'
                'pre_final_round =EXCLUDED.pre_final_round,final_round =EXCLUDED.final_round,no_show =EXCLUDED.no_show;',
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
    'placements_round_progress_dag',
    default_args=default_args,
    description='A DAG for Placements Company Course User mapping Progress',
    schedule_interval='0 14 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS placements_round_progress(
            company_course_user_mapping_progress_id bigint not null PRIMARY KEY,
            company_course_user_mapping_id bigint,
            created_at TIMESTAMP,
            created_by_id bigint,
            description varchar(5000),
            start_timestamp TIMESTAMP,
            end_timestamp TIMESTAMP,
            feedback varchar(600),
            hash varchar(15),
            questions_asked varchar(10000),
            reason_to_not_attending varchar(700),
            rescheduled_at TIMESTAMP,
            status int,
            student_evaluation int,
            timestamp_added_at TIMESTAMP,
            title varchar(125),
            round_type int,
            url varchar(250),
            will_student_attend int,
            your_experience varchar(2000),
            did_student_attend int,
            reason_not_attended varchar(50),
            round_1 boolean,
            round_2 boolean,
            round_3 boolean,
            round_4 boolean,
            pre_final_round boolean,
            final_round boolean,
            no_show boolean
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
            distinct placements_companycourseusermappingprogress.id as company_course_user_mapping_progress_id,
            placements_companycourseusermappingprogress.company_course_user_mapping_id,
            placements_companycourseusermappingprogress.created_at,
            placements_companycourseusermappingprogress.created_by_id,
            placements_companycourseusermappingprogress.description,
            placements_companycourseusermappingprogress.start_timestamp,
            placements_companycourseusermappingprogress.end_timestamp,
            placements_companycourseusermappingprogress.feedback,
            placements_companycourseusermappingprogress.hash,
            placements_companycourseusermappingprogress.questions_asked,
            placements_companycourseusermappingprogress.reason_to_not_attending,
            placements_companycourseusermappingprogress.rescheduled_at,
            placements_companycourseusermappingprogress.status,
            placements_companycourseusermappingprogress.student_evaluation,
            placements_companycourseusermappingprogress.timestamp_added_at,
            placements_companycourseusermappingprogress.title,
            placements_companycourseusermappingprogress.type as round_type,
            placements_companycourseusermappingprogress.url,
            placements_companycourseusermappingprogress.will_student_attend,
            placements_companycourseusermappingprogress.your_experience,
            placements_companycourseusermappingprogress.did_student_attend,
            placements_companycourseusermappingprogress.reason_not_attended,
            case when  r1.label_id = 408 then true else false end as round_1,
            case when  r2.label_id = 409 then true else false end as round_2,
            case when  r3.label_id = 410 then true else false end as round_3,
            case when  r4.label_id = 411 then true else false end as round_4,
            case when  pf.label_id = 412 then true else false end as pre_final_round,
            case when  final.label_id = 413 then true else false end as final_round,
            case when  no_show.label_id = 415 then true else false end as no_show
            from placements_companycourseusermappingprogress
            left join placements_companycourseusermappingprogresslabelmapping as r1 on r1.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and r1.label_id in (408)
            left join placements_companycourseusermappingprogresslabelmapping as r2 on r2.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and r2.label_id in (409)
            left join placements_companycourseusermappingprogresslabelmapping as r3 on r3.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and r3.label_id in (410)
            left join placements_companycourseusermappingprogresslabelmapping as r4 on r4.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and r4.label_id in (411)
            left join placements_companycourseusermappingprogresslabelmapping as pf  on pf.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and pf.label_id in (412)
            left join placements_companycourseusermappingprogresslabelmapping as final  on final.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and final.label_id in (413)
            left join placements_companycourseusermappingprogresslabelmapping as no_show  on no_show.company_course_user_mapping_progress_id = placements_companycourseusermappingprogress.id and no_show.label_id in (415)
            order by 1
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