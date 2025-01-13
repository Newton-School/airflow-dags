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
            '''
            INSERT INTO placements_company (
                company_id,
                company_name,
                company_type,
                created_at,
                created_by_id,
                information,
                remark,
                slug,
                status,
                updated_at,
                topic_id,
                key_account_manager_1,
                key_account_manager_2,
                sales_poc_1,
                sales_poc_2
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_id) DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                topic_id = EXCLUDED.topic_id,
                key_account_manager_1 = EXCLUDED.key_account_manager_1,
                key_account_manager_2 = EXCLUDED.key_account_manager_2,
                sales_poc_1 = EXCLUDED.sales_poc_1,
                sales_poc_2 = EXCLUDED.sales_poc_2,
                information = EXCLUDED.information,
                remark = EXCLUDED.remark,
                status = EXCLUDED.status,
                company_name = EXCLUDED.company_name,
                company_type = EXCLUDED.company_type;
            ''',
            (
                transform_row[0],  # company_id
                transform_row[1],  # company_name
                transform_row[2],  # company_type
                transform_row[3],  # created_at
                transform_row[4],  # created_by_id
                transform_row[5],  # information
                transform_row[6],  # remark
                transform_row[7],  # slug
                transform_row[8],  # status
                transform_row[9],  # updated_at
                transform_row[10], # topic_id
                transform_row[11], # key_account_manager_1
                transform_row[12], # key_account_manager_2
                transform_row[13], # sales_poc_1
                transform_row[14]  # sales_poc_2
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
                company_id BIGINT NOT NULL PRIMARY KEY,
                company_name VARCHAR(256),
                company_type VARCHAR(10),
                created_at TIMESTAMP,
                created_by_id BIGINT,
                information VARCHAR(4500),
                remark VARCHAR(3000),
                slug VARCHAR(128),
                status INT,
                updated_at TIMESTAMP,
                topic_id BIGINT,
                key_account_manager_1 VARCHAR(128),
                key_account_manager_2 VARCHAR(128),
                sales_poc_1 VARCHAR(128),
                sales_poc_2 VARCHAR(128)
            );
    ''',
    dag=dag
)

alter_table_v1 = PostgresOperator(
    task_id='alter_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        DO $$
        BEGIN
            -- Check if the 'information' column is not of type TEXT
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'placements_company'
                  AND column_name = 'information'
                  AND data_type != 'text'
            ) THEN
                -- Alter 'information' column to TEXT
                ALTER TABLE placements_company ALTER COLUMN information TYPE TEXT;
            END IF;

            -- Check if the 'remark' column is not of type TEXT
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'placements_company'
                  AND column_name = 'remark'
                  AND data_type != 'text'
            ) THEN
                -- Alter 'remark' column to TEXT
                ALTER TABLE placements_company ALTER COLUMN remark TYPE TEXT;
            END IF;
        END $$;
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''WITH enterprise AS (
                SELECT DISTINCT 
                    company_id
                FROM 
                    placements_company
                LEFT JOIN 
                    placements_companylabelmapping ON placements_companylabelmapping.company_id = placements_company.id
                LEFT JOIN 
                    technologies_label ON technologies_label.id = placements_companylabelmapping.label_id
                WHERE 
                    placements_companylabelmapping.label_id = 395 
                    AND technologies_label.name = 'Enterprise'
            ),

            kam AS (
                SELECT DISTINCT 
                    company_id, 
                    CONCAT(first_name, ' ', last_name) AS key_account_manager,
                    RANK() OVER (PARTITION BY company_id ORDER BY CONCAT(first_name, ' ', last_name)) AS rn
                FROM 
                    placements_companyplacementcoordinatormapping
                LEFT JOIN 
                    trainers_placementcoordinator ON trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
                LEFT JOIN 
                    auth_user ON trainers_placementcoordinator.user_id = auth_user.id
                WHERE 
                    placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 2
                ORDER BY 
                    company_id
            ),

            sales AS (
                SELECT DISTINCT 
                    company_id, 
                    CONCAT(first_name, ' ', last_name) AS sales_poc,
                    RANK() OVER (PARTITION BY company_id ORDER BY CONCAT(first_name, ' ', last_name)) AS rn
                FROM 
                    placements_companyplacementcoordinatormapping
                LEFT JOIN 
                    trainers_placementcoordinator ON trainers_placementcoordinator.id = placements_companyplacementcoordinatormapping.placement_coordinator_id
                LEFT JOIN 
                    auth_user ON trainers_placementcoordinator.user_id = auth_user.id
                WHERE 
                    placements_companyplacementcoordinatormapping.company_placement_coordinator_mapping_type = 1
                ORDER BY 
                    company_id
            )

            SELECT DISTINCT 
                placements_company.id AS company_id,
                placements_company.title AS company_name,
                CASE 
                    WHEN enterprise.company_id IS NOT NULL THEN 'Enterprise'
                    ELSE 'Startup' 
                END AS company_type,
                placements_company.created_at,
                placements_company.created_by_id,
                placements_company.description AS information,
                placements_company.remark,
                placements_company.slug,
                placements_company.status,
                placements_company.updated_at,
                placements_company.topic_id,
                kam1.key_account_manager AS key_account_manager_1,
                kam2.key_account_manager AS key_account_manager_2,
                sales1.sales_poc AS sales_poc_1,
                sales2.sales_poc AS sales_poc_2
            FROM 
                placements_company
            LEFT JOIN 
                enterprise ON enterprise.company_id = placements_company.id
            LEFT JOIN 
                kam AS kam1 ON kam1.company_id = placements_company.id AND kam1.rn = 1
            LEFT JOIN 
                kam AS kam2 ON kam2.company_id = placements_company.id AND kam2.rn = 2
            LEFT JOIN 
                sales AS sales1 ON sales1.company_id = placements_company.id AND sales1.rn = 1
            LEFT JOIN 
                sales AS sales2 ON sales2.company_id = placements_company.id AND sales2.rn = 2
            ORDER BY 
                company_id;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
create_table >> alter_table_v1 >> transform_data >> extract_python_data