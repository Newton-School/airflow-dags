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
                'INSERT INTO placements_company (company_id,company_name,company_type,created_at,created_by_id,'
                'information,remark,slug,status,updated_at,topic_id,key_account_manager_1,'
                'key_account_manager_2,sales_poc_1,sales_poc_2) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (company_id) do update set updated_at =EXCLUDED.updated_at,topic_id =EXCLUDED.topic_id,'
                'key_account_manager_1 =EXCLUDED.key_account_manager_1,'
                'key_account_manager_2 =EXCLUDED.key_account_manager_2,sales_poc_1 =EXCLUDED.sales_poc_1,'
                'sales_poc_2 =EXCLUDED.sales_poc_2,information =EXCLUDED.information,remark =EXCLUDED.remark,'
                'status =EXCLUDED.status,company_name =EXCLUDED.company_name,company_type =EXCLUDED.company_type ;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'placements_company_dag',
    default_args=default_args,
    description='A DAG for placements company details',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS placements_company (
            company_id bigint not null PRIMARY KEY,
            company_name varchar(256),
            company_type varchar(10),
            created_at TIMESTAMP,
            created_by_id bigint,
            information varchar(4500),
            remark varchar(3000),
            slug varchar(128),
            status int,
            updated_at TIMESTAMP,
            topic_id bigint,
            key_account_manager_1 varchar(128),
            key_account_manager_2 varchar(128),
            sales_poc_1 varchar(128),
            sales_poc_2 varchar(128)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with enterprise as(
            select distinct company_id
            from placements_company
            left join placements_companylabelmapping on placements_companylabelmapping.company_id = placements_company.id
            left join technologies_label on technologies_label.id = placements_companylabelmapping.label_id
            where placements_companylabelmapping.label_id = 395 and technologies_label.name = 'Enterprise'
            ),
            kam as(
            select 
            distinct company_id, concat(first_name,' ',last_name) as key_account_manager,
            rank() over (partition by company_id order by concat(first_name,' ',last_name)) as rn
            from placements_companyplacementcoordinatormapping
            left join trainers_placementcoordinator on trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
            left join auth_user on trainers_placementcoordinator.user_id = auth_user.id
            where placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 2
            order by 1
            ),
            sales as(
            select 
            distinct company_id, concat(first_name,' ',last_name) as sales_poc,
            rank() over (partition by company_id order by concat(first_name,' ',last_name)) as rn
            from placements_companyplacementcoordinatormapping
            left join trainers_placementcoordinator on trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
            left join auth_user on trainers_placementcoordinator.user_id = auth_user.id
            where placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 1
            order by 1
            )
            select
            distinct placements_company.id as company_id,
            placements_company.title as company_name,
            case when enterprise.company_id is not null then 'Enterprise'
            else 'Startup' end as company_type,
            placements_company.created_at,
            placements_company.created_by_id,
            placements_company.description as information,
            placements_company.remark,
            placements_company.slug,
            placements_company.status,
            placements_company.updated_at,
            placements_company.topic_id,
            kam1.key_account_manager as key_account_manager_1,
            kam2.key_account_manager as key_account_manager_2,
            sales1.sales_poc as sales_poc_1,
            sales2.sales_poc as sales_poc_2
            from placements_company
            left join enterprise on enterprise.company_id = placements_company.id
            left join kam as kam1 on kam1.company_id = placements_company.id and kam1.rn = 1
            left join kam as kam2 on kam2.company_id = placements_company.id and kam2.rn = 2
            left join sales as sales1 on sales1.company_id = placements_company.id and sales1.rn = 1
            left join sales as sales2 on sales2.company_id = placements_company.id and sales2.rn = 2
            order by 1;
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