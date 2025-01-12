from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

# Python function to extract data and insert into the table
def extract_data_to_nested(**kwargs):
    def clean_input(data_type, data_value):
        if data_type == 'string':
            return 'null' if not data_value else f'\"{data_value}\"'
        elif data_type == 'datetime':
            return 'null' if not data_value else f'CAST(\'{data_value}\' AS TIMESTAMP)'
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
            INSERT INTO pop_data_v1 (
                email, course_id, amount_paid, bucket, payment_date, cum_created
            ) VALUES (%s, %s, %s, %s, %s, %s);
            ''',
            (
                transform_row[0],  # email
                transform_row[1],  # course_id
                transform_row[2],  # amount_paid
                transform_row[3],  # bucket
                transform_row[4],  # payment_date
                transform_row[5],  # cum_created
            )
        )
    pg_conn.commit()

# Define the DAG
dag = DAG(
    'pop_data_DAG_v1',
    default_args=default_args,
    description='DAG to process and store POP user data',
    schedule_interval='30 * * * *',
    catchup=False,
)

# Task to drop the table if it exists
drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        DROP TABLE IF EXISTS pop_data_v1;
    ''',
    dag=dag,
)

# Task to create the table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''
        CREATE TABLE IF NOT EXISTS pop_data_v1 (
            email VARCHAR(512),
            course_id INT,
            amount_paid DOUBLE PRECISION,
            bucket VARCHAR(256),
            payment_date DATE,
            cum_created DATE
        );
    ''',
    dag=dag,
)

# Task to transform data
transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        WITH deductions AS (
            SELECT
                course_user_invoice_mapping_id,
                MIN(
                    CASE 
                        WHEN deduction_id = 33 AND valid_till IS NOT NULL THEN 1
                        WHEN deduction_id = 33 THEN 2
                        WHEN deduction_id = 10 AND valid_till IS NOT NULL THEN 3
                        WHEN deduction_id = 10 THEN 4
                        WHEN deduction_id = 4 AND valid_till IS NOT NULL THEN 5
                        WHEN deduction_id = 4 THEN 6
                        WHEN deduction_id = 8 AND valid_till IS NOT NULL THEN 7
                        WHEN deduction_id = 8 THEN 8
                    END
                ) AS weight
            FROM invoices_courseuserinvoiceextradeductionmapping
            GROUP BY course_user_invoice_mapping_id
        )
        SELECT 
            auth_user.email, 
            invoices_courseinvoicetemplatemapping.course_id, 
            invoices_courseuserinvoicemapping.paid_amount AS amount_paid,
            CASE 
                WHEN deductions.weight = 1 THEN '99 Timer'
                WHEN deductions.weight = 2 THEN '99 Paid'
                WHEN deductions.weight = 3 THEN '249 Timer'
                WHEN deductions.weight = 4 THEN '249 Paid'
                WHEN deductions.weight = 5 THEN '499 Timer'
                WHEN deductions.weight = 6 THEN '499 Paid'
                WHEN deductions.weight = 7 THEN '999 Timer'
                WHEN deductions.weight = 8 THEN '999 Paid'
            END AS bucket,
            DATE(payments_courseuserinvoicepaymentattempt.created_at) AS payment_date,
            DATE(courses_courseusermapping.created_at) AS cum_created
        FROM invoices_courseuserinvoicemapping 
        INNER JOIN invoices_courseinvoicetemplatemapping 
            ON invoices_courseuserinvoicemapping.course_invoice_template_mapping_id = invoices_courseinvoicetemplatemapping.id
        INNER JOIN courses_course 
            ON invoices_courseinvoicetemplatemapping.course_id = courses_course.id
        INNER JOIN invoices_invoicetemplate 
            ON invoices_courseinvoicetemplatemapping.invoice_template_id = invoices_invoicetemplate.id
        LEFT OUTER JOIN courses_courseusermapping 
            ON invoices_courseuserinvoicemapping.course_user_mapping_id = courses_courseusermapping.id
        LEFT OUTER JOIN auth_user 
            ON courses_courseusermapping.user_id = auth_user.id
        LEFT JOIN payments_courseuserinvoicepaymentmapping 
            ON payments_courseuserinvoicepaymentmapping.course_user_invoice_mapping_id = invoices_courseuserinvoicemapping.id
        LEFT JOIN payments_courseuserinvoicepaymentattempt 
            ON payments_courseuserinvoicepaymentattempt.course_user_invoice_payment_mapping_id = payments_courseuserinvoicepaymentmapping.id
            AND payments_courseuserinvoicepaymentattempt.payment_status = 3
        LEFT JOIN deductions 
            ON deductions.course_user_invoice_mapping_id = invoices_courseuserinvoicemapping.id
        WHERE 
            courses_course.course_structure_id IN (14) AND 
            invoices_invoicetemplate.type = 'ADMISSION_PROCESS_BOOKING_FEE' AND
            auth_user.email NOT LIKE '%newtonschool.co' AND 
            deductions.weight IN (1, 2, 3, 4, 5, 6, 7, 8)
        ORDER BY 6 DESC;
    ''',
    dag=dag,
)

# Task to extract and load data using Python
extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
drop_table >> create_table >> transform_data >> extract_python_data