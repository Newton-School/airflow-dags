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
            'INSERT INTO pop_data_v1 ('
                'email,course_id,amount_paid,bucket,payment_date,cum_created)'
                'VALUES (%s,%s,%s,%s,%s,%s);',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
            )
        )
    pg_conn.commit()


dag = DAG(
    'pop_data_DAG_v1',
    default_args=default_args,
    description='POP users data',
    schedule_interval='30 * * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS pop_data_v1 (
            email varchar(512),
            course_id int,
            amount_paid double precision,
            bucket varchar(256),
            payment_date DATE,
            cum_created DATE
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
        WITH A AS (
    SELECT 
        first_name,
        email,
        phone,
        "Click_Payment_button",
        "Final Paid",
        Amount_Paid,
        course_id,
        cum_created,
        CASE 
            WHEN "99 Timer" IS NOT NULL THEN '99 Timer'
            WHEN "99 Paid" IS NOT NULL AND "99 Timer" IS NULL THEN '99 Paid'
            WHEN "249 Timer" IS NOT NULL THEN '249 Timer'
            WHEN "249 Paid" IS NOT NULL AND "249 Timer" IS NULL THEN '249 Paid'
            WHEN "499 Timer" IS NOT NULL THEN '499 Timer'
            WHEN "499 Paid" IS NOT NULL AND "499 Timer" IS NULL THEN '499 Paid'
            WHEN "999 Timer" IS NOT NULL THEN '999 Timer'
            WHEN "999 Paid" IS NOT NULL AND "999 Timer" IS NULL THEN '999 Paid'
            
        END AS Bucket,
        created_at,
        invoice_date,
        payment_date
    FROM (
        SELECT
            auth_user.first_name,
            auth_user.email,
            auth_user.id AS user_id,
            users_userprofile.phone,
            "courses_courseusermapping".created_at,
            courses_course.id AS course_id,
            invoices_courseuserinvoicemapping.created_at AS invoice_date,
            DATE(courses_courseusermapping.created_at) AS cum_created,
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 4 AND U0."valid_till" IS NOT NULL AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "499 Timer",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 4 AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "499 Paid",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 8 AND U0."valid_till" IS NOT NULL AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "999 Timer",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 8 AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "999 Paid",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 10 AND U0."valid_till" IS NOT NULL AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "249 Timer",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 10 AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "249 Paid",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 33 AND U0."valid_till" IS NOT NULL AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "99 Timer",
            (SELECT U0."deduction_id" 
             FROM "invoices_courseuserinvoiceextradeductionmapping" U0 
             WHERE U0."deduction_id" = 33 AND U0."course_user_invoice_mapping_id" = invoices_courseuserinvoicemapping.id 
             LIMIT 1) AS "99 Paid",
             
            payments_courseuserinvoicepaymentmapping.course_user_invoice_mapping_id AS "Click_Payment_button",
            payments_courseuserinvoicepaymentattempt.course_user_invoice_payment_mapping_id AS "Final Paid",
            CAST(CAST(payments_courseuserinvoicepaymentattempt.payment_gateway_response->'amount' AS VARCHAR) AS INT)/100 AS Amount_Paid,
            payments_courseuserinvoicepaymentattempt.created_at AS payment_date
        FROM invoices_courseuserinvoicemapping 
        LEFT JOIN payments_courseuserinvoicepaymentmapping ON payments_courseuserinvoicepaymentmapping.course_user_invoice_mapping_id = invoices_courseuserinvoicemapping.id
        LEFT JOIN payments_courseuserinvoicepaymentattempt ON payments_courseuserinvoicepaymentattempt.course_user_invoice_payment_mapping_id = payments_courseuserinvoicepaymentmapping.id AND payments_courseuserinvoicepaymentattempt.payment_status = 3
        LEFT JOIN invoices_courseinvoicetemplatemapping ON invoices_courseuserinvoicemapping.course_invoice_template_mapping_id = invoices_courseinvoicetemplatemapping.id
        LEFT JOIN invoices_invoicetemplate ON invoices_invoicetemplate.id = invoices_courseinvoicetemplatemapping.invoice_template_id
        LEFT JOIN "courses_courseusermapping" ON invoices_courseuserinvoicemapping.course_user_mapping_id = "courses_courseusermapping".id
        LEFT JOIN "courses_course" ON "courses_courseusermapping".course_id = "courses_course".id
        LEFT JOIN auth_user ON courses_courseusermapping.user_id = auth_user.id
        LEFT JOIN users_userprofile ON users_userprofile.user_id = auth_user.id
        WHERE invoices_invoicetemplate.type = 'ADMISSION_PROCESS_BOOKING_FEE'
        AND courses_course.id IN (800,818,819,820,821,822,823,824,825,826,1040,1041,1042)
        AND "courses_courseusermapping".user_id IS NOT NULL
        AND auth_user.email NOT LIKE '%newtonschool.co'
    ) AS t
)
SELECT DISTINCT 
    email,
    course_id,
    amount_paid,
    bucket,
    DATE(payment_date) AS payment_date,
    DATE(cum_created) AS cum_created
FROM A
WHERE Bucket IN ('499 Paid','499 Timer', '999 Paid','999 Timer','249 Timer','249 Paid','99 Paid','99 Timer')
AND course_id IN (800,818,819,820,821,822,823,824,825,826,1040,1041,1042)
ORDER BY 1, 2;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_result_db',
    sql='''DROP TABLE IF EXISTS pop_data_v1;
    ''',
    dag=dag
)

drop_table >> create_table >> transform_data >> extract_python_data
