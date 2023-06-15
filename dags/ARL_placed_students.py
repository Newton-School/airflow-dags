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
            'INSERT INTO arl_placed_students (Offer_type,Placement_id,user_id,phone_number,Full_name,'
            'username,Email,ISA,Date_of_placement,Joining_date,CTC,Company,company_id,Job_Role,course_name,'
            'Status,institute,degree,field,kam,sales_Manager,final_mentor,week,gender,pccumid,referred_by,'
            'placement_role_id,company_type) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (pccumid) do update set Offer_type=EXCLUDED.Offer_type,'
            'Placement_id=EXCLUDED.Placement_id,user_id=EXCLUDED.user_id,phone_number=EXCLUDED.phone_number,'
            'Full_name=EXCLUDED.Full_name,username=EXCLUDED.username,Email=EXCLUDED.Email,ISA=EXCLUDED.ISA,'
            'Date_of_placement=EXCLUDED.Date_of_placement,Joining_date=EXCLUDED.Joining_date,CTC=EXCLUDED.CTC,'
            'Company=EXCLUDED.Company,company_id=EXCLUDED.company_id,Job_Role=EXCLUDED.Job_Role,'
            'course_name=EXCLUDED.course_name,Status=EXCLUDED.Status,institute=EXCLUDED.institute,degree=EXCLUDED.degree,'
            'field=EXCLUDED.field,kam=EXCLUDED.kam,sales_Manager=EXCLUDED.sales_Manager,final_mentor=EXCLUDED.final_mentor,'
            'week=EXCLUDED.week,gender=EXCLUDED.gender,referred_by=EXCLUDED.referred_by,'
            'placement_role_id=EXCLUDED.placement_role_id,company_type=EXCLUDED.company_type;',
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

            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_placed_students_DAG',
    default_args=default_args,
    description='A DAG for placed students details',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_placed_students (
            Offer_type varchar(50),
            Placement_id bigint,
            user_id int,
            phone_number bigint,
            Full_name varchar(400),
            username varchar(100),
            Email varchar(100),
            ISA int,
            Date_of_placement TIMESTAMP,
            Joining_date TIMESTAMP,
            CTC int,
            Company varchar(300),
            company_id int,
            Job_Role varchar(300),
            course_name varchar(200),
            Status varchar(200),
            institute varchar(400),
            degree varchar(200),
            field varchar(200),
            kam varchar(200),
            sales_Manager varchar(200),
            final_mentor varchar(200),
            week DATE,
            gender varchar(20),
            pccumid bigint not null PRIMARY KEY,
            referred_by  varchar(100),
            placement_role_id int,
            company_type varchar(100)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with t0 as(
            select auth_user.id, users_education.school as "Institute",end_date,education_degree.name as degree,education_fieldofstudy.name as field, row_number() over (partition by auth_user.id order by end_date desc)
            from auth_user
            left join users_education on auth_user.id = users_education.user_id
            left join education_degree on users_education.degree_id = education_degree.id
            left join education_fieldofstudy on users_education.field_of_study_id = education_fieldofstudy.id
            where users_education.degree_id not in (26,27)
            ),
            positive_enterprises as(
            select distinct company_id as object_id
            from placements_company
            left join placements_companylabelmapping on placements_companylabelmapping.company_id = placements_company.id
            left join technologies_label on technologies_label.id = placements_companylabelmapping.label_id
            where placements_companylabelmapping.label_id = 395 and technologies_label.name = 'Enterprise'
            ),
            t1 as(
            select id, "Institute",degree, field
            from t0
            where row_number = 1
            ),
            KAM0 as (Select placements_company.id, concat(A.first_name,' ',A.last_name) as Name, row_number() over (partition by placements_company.id)
            from placements_company
            left join placements_companyplacementcoordinatormapping
                on placements_companyplacementcoordinatormapping.company_id =placements_company.id
            left join trainers_placementcoordinator 
                on trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
            left join auth_user A
                on trainers_placementcoordinator.user_id = A.id 
                where placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 2 
            ),
            KAM as (
            select * from KAM0 where row_number = 1
            ),
            Sales_Manager0 as(
            Select placements_company.id, concat(B.first_name, ' ', B.last_name) as Name, row_number() over (partition by placements_company.id)
            from placements_company
            left join placements_companyplacementcoordinatormapping
                on placements_companyplacementcoordinatormapping.company_id =placements_company.id
            left join trainers_placementcoordinator 
                on trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
            left join auth_user B
                on trainers_placementcoordinator.user_id = B.id 
            where placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 1),
            Sales_Manager as (
            select * from Sales_Manager0 where row_number = 1
            ),
            latest_mentor as(
            select id, "Mentor Name"
            from
            (select a.id,tb.created_at,td.created_at,concat(b.first_name,' ', b.last_name) as "Mentor Name",row_number() over (partition by a.id order by tb.created_at desc, td.created_at desc)
            from (select course_user_mapping_id, created_at, sub_batch_id, row_number() over (partition by course_user_mapping_id order by created_at desc)
            from courses_subbatchcourseusermapping
            ) tb
            inner join (
            select sub_batch_id, created_at, course_mentor_mapping_id, row_number() over (partition by sub_batch_id order by created_at desc)
            from trainers_coursementorsubbatchmapping
            ) td on tb.sub_batch_id = td.sub_batch_id
            left join courses_courseusermapping on tb.course_user_mapping_id = courses_courseusermapping.id
            left join auth_user a on a.id = courses_courseusermapping.user_id
            left join trainers_coursementormapping on td.course_mentor_mapping_id = trainers_coursementormapping.id
            left join trainers_mentor on trainers_coursementormapping.mentor_id = trainers_mentor.id
            left join auth_user b on b.id = trainers_mentor.user_id) finalt
            where row_number = 1
            ),
            phone as (select user_id, right(phone,10) as phone_number from users_userprofile ),
            gender as(
            select
            users_userprofile.user_id,
            case 
            when users_userprofile.gender = 1 then 'Male'
            when users_userprofile.gender = 2 then 'Female' end as "Gender"
            from users_userprofile
            ),
            placement as (
            select 
            concat(auth_user.id, placements_company.id) as Placement_id,
            auth_user.id as user_id, 
            phone.phone_number,
            concat(auth_user.first_name,' ', auth_user.last_name) as Full_name, 
            auth_user.username as username, 
            auth_user.email as Email, 
            isa_isacourseusermapping.offered_isa_amount as ISA, 
            placements_companycourseusermapping.placed_at as Date_of_placement, 
            placements_companycourseusermapping.join_timestamp as Joining_date,
            placements_companycourseusermapping.ctc as CTC, 
            placements_company.title as Company, 
            placements_company.id as company_id, 
            placements_jobopening.title as Job_Role, 
            placements_jobopening.placement_role_id,
            courses_course.title,
            placements_companycourseusermapping.status as Status,
            case when placements_companycourseusermapping.status = 3 then 'Candidate Selected'
            when placements_companycourseusermapping.status = 4 then 'Offer Letter Received'
            when placements_companycourseusermapping.status = 5 then 'Offer letter accepted'
            when placements_companycourseusermapping.status = 6 then 'Offer letter Denied'
            when placements_companycourseusermapping.status = 11 then 'Accepted another offer'
            when placements_companycourseusermapping.status = 16 then 'Candidate Resigned'
            when placements_companycourseusermapping.status = 17 then 'Candidate laid off'
            when placements_companycourseusermapping.status = 18 then 'Offer confirmation pending by student'
            when placements_companycourseusermapping.status = 19 then 'Offer confirmed by student'
            when placements_companycourseusermapping.status = 20 then 'Candidate joined company'
            when placements_companycourseusermapping.status = 21 then 'Offer cancelled by company'
            when placements_companycourseusermapping.status = 25 then 'Externally Placed'
            end as Status_title,
            "Institute",
            degree,
            field,
            KAM.Name as "KAM", 
            Sales_Manager.Name as "Sales Manager",
            "Mentor Name" as "Final Mentor",
            courses_course.course_structure_id,
            placements_companycourseusermapping.id as aid,
            concat(a.first_name,' ',a.last_name) as referred_by,
            case when positive_enterprises.object_id is null then 'Startup'
            else 'Enterprise' end as "Startup/Enterprise"
            
            
            from
            placements_companycourseusermapping 
            left join placements_company on placements_company.id=placements_companycourseusermapping.company_id
            left join positive_enterprises on positive_enterprises.object_id = placements_company.id
            left join placements_jobopening on placements_jobopening.id = placements_companycourseusermapping.job_opening_id
            left join courses_courseusermapping CUM on placements_companycourseusermapping.course_user_mapping_id=CUM.id
            left join auth_user on auth_user.id=CUM.user_id
            left join auth_user as a on a.id = placements_companycourseusermapping.created_by_id
            left join courses_course on CUM.course_id=courses_course.id
            left join isa_isacourseusermapping on isa_isacourseusermapping.course_user_mapping_id = CUM.id
            left join t1 on auth_user.id = t1.id
            left join KAM on placements_company.id =KAM.id
            left join Sales_Manager on placements_company.id =Sales_Manager.id
            left join latest_mentor on latest_mentor.id = auth_user.id
            left join phone on phone.user_id = auth_user.id
            left join users_userprofile on users_userprofile.user_id=auth_user.id
            where 
                placements_companycourseusermapping.status in (3,4,6,11,18,21,5,16,17,19,20,25) 
                and placements_companycourseusermapping.placed_at is not null 
            order by user_id,Date_of_placement
            
            ),
            unclean as(
            select 
            
            Case when placement.Status in (5,16,17,19,20) and course_structure_id in (1,18,5) then 'Clean' 
            when placement.Status in (5,16,17,19,20) and course_structure_id in (6,8,11,12,13,19,14,20) then 'University Placement'
            when placement.Status in (3) and CTC >= ISA then 'Not Contacted'
            when placement.Status in (3) and CTC < ISA then 'Unclean (Below ISA)'
            when placement.Status in (4,18) then 'Confirmation Pending by Student'
            when placement.Status in (21,11,6) then 'Unclean' 
            when placement.Status in (25) then 'Externally Placed' 
            end as Offer_type,
            Placement_id,
            user_id,
            phone_number,
            Full_name,
            username, 
            Email,
            ISA, 
            Date_of_placement,
            Joining_date,
            CTC,
            Company, 
            company_id, 
            Job_Role,
            title,
            Status_title,
            "Institute",
            degree,
            field,
            "KAM", 
            "Sales Manager",
            "Final Mentor",
            aid,
            referred_by,
            placement_role_id,
            "Startup/Enterprise"
            from placement
            where placement.Status in (3,4,6,11,18,21,25) 
            order by user_id,Date_of_placement
            ),
            cleanrough as (
            select 
            *,
            RANK() OVER (PARTITION BY user_id ORDER BY Date_of_placement ASC)
            from placement
            where Status in (5,16,17,19,20) 
            order by user_id,Date_of_placement
            ),
            clean as (
            select 
            case when RANK = 1 and course_structure_id in (1,18,5) then 'Clean' 
            when course_structure_id in (6,8,11,12,13,19,14,20) then 'University Placement'
            else 'Alum Placement' end as Offer_type,
            Placement_id,
            user_id,
            phone_number,
            Full_name,
            username, 
            Email,
            ISA, 
            Date_of_placement,
            Joining_date,
            CTC,
            Company, 
            company_id, 
            Job_Role,
            title,
            Status_title,
            "Institute",
            degree,
            field,
            "KAM", 
            "Sales Manager",
            "Final Mentor",
            aid,
            referred_by,
            placement_role_id,
            "Startup/Enterprise"
            from cleanrough
            ),
            final as(
            select * from clean union all select * from unclean
            order by Date_of_placement desc
            )
            select
            final.Offer_type,
            final.Placement_id,
            final.user_id,
            final.phone_number,
            final.Full_name,
            final.username, 
            final.Email,
            final.ISA, 
            placements_companycourseusermapping.placed_at as Date_of_placement,
            final.Joining_date,
            final.CTC,
            final.Company, 
            final.company_id, 
            final.Job_Role,
            final.title as course_name,
            Status_title as Status,
            "Institute" as institute,
            degree,
            field,
            "KAM" as kam, 
            "Sales Manager" as sales_Manager,
            "Final Mentor" as final_mentor,
            date(date_trunc('week',placements_companycourseusermapping.placed_at)) as week,
            gender."Gender" as gender,
            final.aid as pccumid,
            final.referred_by,
            final.placement_role_id,
            "Startup/Enterprise" as company_type
            from final
            join placements_companycourseusermapping on placements_companycourseusermapping.id = final.aid
            left join gender on gender.user_id = final.user_id
            order by Date_of_placement desc;
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