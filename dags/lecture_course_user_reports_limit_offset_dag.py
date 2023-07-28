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

lecture_per_dags = Variable.get("lecture_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'lecture_course_user_reports_limit_offset_dag',
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
            lecture_title varchar(1028),
            mandatory boolean,
            topic_template_id int,
            template_name varchar(255),
            inst_total_time_in_mins int,
            inst_user_id bigint,
            lecture_date DATE,
            live_attendance int,
            recorded_attendance int,
            overall_attendance int,
            total_overlapping_time_in_mins int,
            answer_rating int,
            rating_feedback_answer text,
            lecture_understood_rating int,
            lecture_understanding_feedback_answer text
        );
    ''',
    dag=dag
)

# Leaf Level Abstraction
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(
        task_ids=f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('INSERT INTO lecture_course_user_reports (table_unique_key, user_id, student_name,'
            'lead_type, student_category, course_user_mapping_id, label_mapping_status,'
            'course_id, course_name, course_structure_class, lecture_id, lecture_title,'
            'mandatory, topic_template_id, template_name, inst_total_time_in_mins, inst_user_id,'
            'lecture_date, live_attendance, recorded_attendance,'
            'overall_attendance, total_overlapping_time_in_mins, answer_rating,'
            'rating_feedback_answer,'
            'lecture_understood_rating,'
            'lecture_understanding_feedback_answer)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
            'total_overlapping_time_in_mins = EXCLUDED.total_overlapping_time_in_mins,'
            'answer_rating = EXCLUDED.answer_rating,'
            'rating_feedback_answer = EXCLUDED.rating_feedback_answer,'
            'lecture_understood_rating = EXCLUDED.lecture_understood_rating,'
            'lecture_understanding_feedback_answer = EXCLUDED.lecture_understanding_feedback_answer;',
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
            )
        )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_lecture_sub_dag_func(start_lecture_id, end_lecture_id):
    return PostgresOperator(
        task_id='number_of_rows_per_lecture_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql='''select count(table_unique_key) from
            (with user_raw_data as
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
            group by 1,2,3),
            
        lecture_rating as 
			(select
				user_id,
				entity_object_id as lecture_id,
			    case
			        when ffar.feedback_answer = 'Awesome' then 5
			        when ffar.feedback_answer = 'Good' then 4
			        when ffar.feedback_answer = 'Average' then 3
			        when ffar.feedback_answer = 'Poor' then 2
			        when ffar.feedback_answer = 'Very Poor' then 1
			    end as answer_rating,
			    feedback_answer as rating_feedback_answer
			from
				feedback_form_all_responses ffar
			where ffar.feedback_form_id = 4377 
				and ffar.feedback_question_id = 348
			group by 1,2,3,4),

		lecture_understanding as 
			(select
					user_id,
					entity_object_id as lecture_id,
		        case 
		            when feedback_answer_id = 179 then 1
		            when feedback_answer_id = 180 then 0
		            when feedback_answer_id = 181 then -1
		        end as lecture_understood_rating,
				    feedback_answer as lecture_understanding_feedback_answer
				from
					feedback_form_all_responses ffar
				where ffar.feedback_form_id = 4377 
					and ffar.feedback_question_id = 331
				group by 1,2,3,4)    
            
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
            cast(sum(let.overlapping_time_minutes) as int) as total_overlapping_time_in_mins,
            answer_rating,
            rating_feedback_answer,
            lecture_understood_rating,
            lecture_understanding_feedback_answer
        from
            courses c
        join course_user_mapping cum
            on cum.course_id = c.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26)
                and cum.status in (8,9,11,12,30) and c.course_id in (select distinct wab.lu_course_id from wow_active_batches wab) 
        join lectures l
            on l.course_id = c.course_id and l.start_timestamp >= '2022-07-01'
              and (l.lecture_id between %d and %d)
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
        left join lecture_understanding
        	on lecture_understanding.lecture_id = l.lecture_id 
        		and lecture_understanding.user_id = cum.user_id 
        left join lecture_rating
        	on lecture_rating.lecture_id = l.lecture_id 
        		and lecture_rating.user_id = cum.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,23,24,25,26) count_query;
        ''' % (start_lecture_id, end_lecture_id),
    )

# Python Limit Offset generator
def limit_offset_generator_func(**kwargs):
    ti = kwargs['ti']
    current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    count_cps_rows = ti.xcom_pull(
        task_ids=f'transforming_data_{current_lecture_sub_dag_id}.number_of_rows_per_lecture_sub_dag')
    print(count_cps_rows)
    total_count_rows = count_cps_rows[0][0]
    return {
        "limit": total_count_rows // total_number_of_extraction_cps_dags,
        "offset": current_cps_sub_dag_id * (total_count_rows // total_number_of_extraction_cps_dags) + 1,
    }


def transform_data_per_query(start_lecture_id, end_lecture_id, cps_sub_dag_id, current_lecture_sub_dag_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_lecture_sub_dag_id': current_lecture_sub_dag_id,
            'task_key': f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql=''' with user_raw_data as
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
            group by 1,2,3),
            
        lecture_rating as 
			(select
				user_id,
				entity_object_id as lecture_id,
			    case
			        when ffar.feedback_answer = 'Awesome' then 5
			        when ffar.feedback_answer = 'Good' then 4
			        when ffar.feedback_answer = 'Average' then 3
			        when ffar.feedback_answer = 'Poor' then 2
			        when ffar.feedback_answer = 'Very Poor' then 1
			    end as answer_rating,
			    feedback_answer as rating_feedback_answer
			from
				feedback_form_all_responses ffar
			where ffar.feedback_form_id = 4377 
				and ffar.feedback_question_id = 348
			group by 1,2,3,4),

		lecture_understanding as 
			(select
					user_id,
					entity_object_id as lecture_id,
		        case 
		            when feedback_answer_id = 179 then 1
		            when feedback_answer_id = 180 then 0
		            when feedback_answer_id = 181 then -1
		        end as lecture_understood_rating,
				    feedback_answer as lecture_understanding_feedback_answer
				from
					feedback_form_all_responses ffar
				where ffar.feedback_form_id = 4377 
					and ffar.feedback_question_id = 331
				group by 1,2,3,4)    
            
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
            cast(sum(let.overlapping_time_minutes) as int) as total_overlapping_time_in_mins,
            answer_rating,
            rating_feedback_answer,
            lecture_understood_rating,
            lecture_understanding_feedback_answer
        from
            courses c
        join course_user_mapping cum
            on cum.course_id = c.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26)
                and cum.status in (8,9,11,12,30) and c.course_id in (select distinct wab.lu_course_id from wow_active_batches wab) 
        join lectures l
            on l.course_id = c.course_id and l.start_timestamp >= '2022-07-01'
              and (l.lecture_id between %d and %d)
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
        left join lecture_understanding
        	on lecture_understanding.lecture_id = l.lecture_id 
        		and lecture_understanding.user_id = cum.user_id 
        left join lecture_rating
        	on lecture_rating.lecture_id = l.lecture_id 
        		and lecture_rating.user_id = cum.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,23,24,25,26;
            ''' % (start_lecture_id, end_lecture_id),
    )

for lecture_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{lecture_sub_dag_id}", dag=dag) as lecture_sub_dag_task_group:
        lecture_start_id = lecture_sub_dag_id * int(lecture_per_dags) + 1
        lecture_end_id = (lecture_sub_dag_id + 1) * int(lecture_per_dags)
        number_of_rows_per_lecture_sub_dag = number_of_rows_per_lecture_sub_dag_func(lecture_start_id,
                                                                                           lecture_end_id)

        for cps_sub_dag_id in range(int(total_number_of_extraction_cps_dags)):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_lecture_sub_dag_{lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag) as cps_sub_dag:
                limit_offset_generator = PythonOperator(
                    task_id='limit_offset_generator',
                    python_callable=limit_offset_generator_func,
                    provide_context=True,
                    op_kwargs={
                        'current_lecture_sub_dag_id': lecture_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id,
                    },
                    dag=dag,
                )

                transform_data = transform_data_per_query(lecture_start_id, lecture_end_id, cps_sub_dag_id,
                                                          lecture_sub_dag_id)

                extract_python_data = PythonOperator(
                    task_id='extract_python_data',
                    python_callable=extract_data_to_nested,
                    provide_context=True,
                    op_kwargs={
                        'current_lecture_sub_dag_id': lecture_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id
                    },
                    dag=dag,
                )

                limit_offset_generator >> transform_data >> extract_python_data

            number_of_rows_per_lecture_sub_dag >> cps_sub_dag

    create_table >> lecture_sub_dag_task_group