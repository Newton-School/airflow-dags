"""
Airflow DAG for fetching LeadSquare leads and storing them in PostgreSQL.

This DAG:
1. Fetches leads from LeadSquare API
2. Maps the API response fields to database columns
3. Stores data in lsq_leads_v2 table
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from leadsquare.client import LSQClient

# Configuration constants
POSTGRES_CONN_ID = "postgres_lsq_leads"
RATE_LIMIT_DELAY = 5  # seconds between API calls

logger = logging.getLogger(__name__)


class LSQLeadsManager:
    """Manager class for LSQ leads operations."""

    # Field mapping from API to database columns
    FIELD_MAPPING = {
            'ProspectID': 'prospectid',
            'Score': 'score',
            'Origin': 'origin',
            'mx_ICP': 'mx_icp',
            'LeadAge': 'leadage',
            'mx_City': 'mx_city',
            'LastName': 'lastname',
            'CreatedBy': 'createdby',
            'CreatedOn': 'createdon',
            'FirstName': 'firstname',
            'mx_Bucket': 'mx_bucket',
            'mx_status': 'mx_status',
            'LeadNumber': 'leadnumber',
            'ModifiedOn': 'modifiedon',
            'OwnerIdName': 'owneridname',
            'mx_RFD_Date': 'mx_rfd_date',
            'EmailAddress': 'emailaddress',
            'mx_Identifer': 'mx_identifer',
            'mx_Lead_Type': 'mx_lead_type',
            'mx_Substatus': 'mx_substatus',
            'ProspectStage': 'prospectstage',
            'mx_Lead_Owner': 'mx_lead_owner',
            'mx_Network_Id': 'mx_network_id',
            'mx_UTM_Medium': 'mx_utm_medium',
            'mx_UTM_Source': 'mx_utm_source',
            'mx_total_fees': 'mx_total_fees',
            'QualityScore01': 'qualityscore01',
            'mx_UTM_Referer': 'mx_utm_referer',
            'mx_cibil_check': 'mx_cibil_check',
            'mx_College_City': 'mx_college_city',
            'mx_College_Name': 'mx_college_name',
            'mx_UTM_Campaign': 'mx_utm_campaign',
            'mx_doc_approved': 'mx_doc_approved',
            'mx_Date_Of_Birth': 'mx_date_of_birth',
            'mx_doc_collected': 'mx_doc_collected',
            'mx_total_revenue': 'mx_total_revenue',
            'mx_Graduation_Year': 'mx_graduation_year',
            'mx_Organic_Inbound': 'mx_organic_inbound',
            'mx_Priority_Status': 'mx_priority_status',
            'mx_Work_Experience': 'mx_work_experience',
            'mx_Last_Call_Status': 'mx_last_call_status',
            'mx_Mid_Funnel_Count': 'mx_mid_funnel_count',
            'mx_Test_Date_n_Time': 'mx_test_date_n_time',
            'mx_Reactivation_Date': 'mx_reactivation_date',
            'mx_Lead_Quality_Grade': 'mx_lead_quality_grade',
            'mx_Mid_Funnel_Buckets': 'mx_mid_funnel_buckets',
            'mx_Squadstack_Calling': 'mx_squadstack_calling',
            'mx_Entrance_exam_Marks': 'mx_entrance_exam_marks',
            'mx_Reactivation_Bucket': 'mx_reactivation_bucket',
            'mx_Last_Call_Sub_Status': 'mx_last_call_sub_status',
            'mx_Lead_Inherent_Intent': 'mx_lead_inherent_intent',
            'mx_Highest_Qualification': 'mx_highest_qualification',
            'mx_Source_Intended_Course': 'mx_source_intended_course',
            'mx_Product_Graduation_Year': 'mx_product_graduation_year',
            'mx_Year_of_Passing_in_Text': 'mx_year_of_passing_in_text',
            'mx_Current_Interested_Course': 'mx_current_interested_course',
            'mx_Last_Call_Connection_Status': 'mx_last_call_connection_status',
            'mx_Squadstack_Qualification_Status': 'mx_squadstack_qualification_status',
            'mx_Phoenix_Identifer': 'mx_phoenix_identifer',
            'mx_Lead_Status': 'mx_lead_status',
            'mx_PMM_Identifier': 'mx_pmm_identifier',
            'mx_Prospect_Status': 'mx_prospect_status',
            'mx_Reactivation_Source': 'mx_reactivation_source',
            'mx_Phoenix_Lead_Assigned_Date': 'mx_phoenix_lead_assigned_date',
            'OwnerId': 'ownerid'
    }

    def __init__(self, pg_hook: PostgresHook, lsq_client: LSQClient):
        """Initialize manager with database hook and LSQ client.

        Args:
            pg_hook: PostgreSQL hook for database operations
            lsq_client: LSQ client for API calls
        """
        self.pg_hook = pg_hook
        self.lsq_client = lsq_client

    def create_table(self) -> None:
        """Create lsq_leads_v2 table if it doesn't exist."""
        create_table_query = """
                             CREATE TABLE IF NOT EXISTS lsq_leads_v2
                             (
                                 score
                                 TEXT,
                                 origin
                                 TEXT,
                                 mx_icp
                                 TEXT,
                                 leadage
                                 TEXT,
                                 mx_city
                                 TEXT,
                                 lastname
                                 TEXT,
                                 createdby
                                 TEXT,
                                 createdon
                                 TEXT,
                                 firstname
                                 TEXT,
                                 mx_bucket
                                 TEXT,
                                 mx_status
                                 TEXT,
                                 leadnumber
                                 TEXT,
                                 modifiedon
                                 TEXT,
                                 prospectid
                                 TEXT
                                 PRIMARY
                                 KEY,
                                 owneridname
                                 TEXT,
                                 mx_rfd_date
                                 TEXT,
                                 emailaddress
                                 TEXT,
                                 mx_identifer
                                 TEXT,
                                 mx_lead_type
                                 TEXT,
                                 mx_substatus
                                 TEXT,
                                 prospectstage
                                 TEXT,
                                 mx_lead_owner
                                 TEXT,
                                 mx_network_id
                                 TEXT,
                                 mx_utm_medium
                                 TEXT,
                                 mx_utm_source
                                 TEXT,
                                 mx_total_fees
                                 TEXT,
                                 qualityscore01
                                 TEXT,
                                 mx_utm_referer
                                 TEXT,
                                 mx_cibil_check
                                 TEXT,
                                 mx_college_city
                                 TEXT,
                                 mx_college_name
                                 TEXT,
                                 mx_utm_campaign
                                 TEXT,
                                 mx_doc_approved
                                 TEXT,
                                 mx_date_of_birth
                                 TEXT,
                                 mx_doc_collected
                                 TEXT,
                                 mx_total_revenue
                                 TEXT,
                                 mx_graduation_year
                                 TEXT,
                                 mx_organic_inbound
                                 TEXT,
                                 mx_priority_status
                                 TEXT,
                                 mx_work_experience
                                 TEXT,
                                 mx_last_call_status
                                 TEXT,
                                 mx_mid_funnel_count
                                 TEXT,
                                 mx_test_date_n_time
                                 TEXT,
                                 mx_reactivation_date
                                 TEXT,
                                 mx_lead_quality_grade
                                 TEXT,
                                 mx_mid_funnel_buckets
                                 TEXT,
                                 mx_squadstack_calling
                                 TEXT,
                                 mx_entrance_exam_marks
                                 TEXT,
                                 mx_reactivation_bucket
                                 TEXT,
                                 mx_last_call_sub_status
                                 TEXT,
                                 mx_lead_inherent_intent
                                 TEXT,
                                 mx_highest_qualification
                                 TEXT,
                                 mx_source_intended_course
                                 TEXT,
                                 mx_product_graduation_year
                                 TEXT,
                                 mx_year_of_passing_in_text
                                 TEXT,
                                 mx_current_interested_course
                                 TEXT,
                                 mx_last_call_connection_status
                                 TEXT,
                                 mx_squadstack_qualification_status
                                 TEXT,
                                 mx_phoenix_identifer
                                 TEXT,
                                 mx_lead_status
                                 TEXT,
                                 mx_pmm_identifier
                                 TEXT,
                                 mx_prospect_status
                                 TEXT,
                                 mx_reactivation_source
                                 TEXT,
                                 mx_phoenix_lead_assigned_date
                                 TEXT,
                                 ownerid
                                 TEXT,
                                 airflow_created_on
                                 TIMESTAMP
                                 DEFAULT
                                 CURRENT_TIMESTAMP,
                                 airflow_modified_on
                                 TIMESTAMP
                                 DEFAULT
                                 CURRENT_TIMESTAMP
                             );

                             CREATE INDEX IF NOT EXISTS idx_lsq_leads_v2_modifiedon ON lsq_leads_v2(modifiedon);
                             CREATE INDEX IF NOT EXISTS idx_lsq_leads_v2_createdon ON lsq_leads_v2(createdon);
                             CREATE INDEX IF NOT EXISTS idx_lsq_leads_v2_emailaddress ON lsq_leads_v2(emailaddress); \
                             """

        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()

        logger.info("Table lsq_leads_v2 created or verified")

    def fetch_and_store_leads(self, from_date: str, to_date: str) -> int:
        """Fetch leads from LSQ API and store in database.

        Args:
            from_date: Start date in format 'YYYY-MM-DD HH:MM:SS'
            to_date: End date in format 'YYYY-MM-DD HH:MM:SS'

        Returns:
            Total number of leads processed
        """
        total_leads = 0
        page_index = 1

        while True:
            try:
                logger.info(f"Fetching page {page_index} for date range {from_date} to {to_date}")

                response = self.lsq_client.fetch_leads(
                        from_date=from_date,
                        to_date=to_date,
                        page_index=page_index
                )

                record_count = response.get('RecordCount', 0)
                logger.info(f"Page {page_index}: {record_count} leads found")

                if record_count == 0:
                    logger.info("No more leads to fetch")
                    break

                leads = response.get('Leads', [])
                if not leads:
                    break

                # Transform and store leads
                self._store_leads(leads)
                total_leads += len(leads)

                logger.info(f"Stored {len(leads)} leads. Total: {total_leads}")

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)
                page_index += 1

            except Exception as e:
                logger.error(f"Error fetching page {page_index}: {str(e)}")
                raise

        return total_leads

    def _store_leads(self, leads: List[Dict]) -> None:
        """Store leads in database with upsert logic.

        Args:
            leads: List of lead dictionaries from API
        """
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for lead in leads:
                    lead_data = self._transform_lead(lead)
                    if lead_data and lead_data.get('prospectid'):
                        self._upsert_lead(cursor, lead_data)
                conn.commit()

    def _transform_lead(self, lead: Dict) -> Dict[str, Optional[str]]:
        """Transform API lead data to database format.

        Args:
            lead: Lead dictionary from API with LeadPropertyList

        Returns:
            Dictionary with database column names as keys
        """
        lead_properties = lead.get('LeadPropertyList', [])

        # Create a mapping of attribute to value
        property_map = {
                prop['Attribute']: prop['Value']
                for prop in lead_properties
        }

        # Transform to database format
        transformed = {}
        for api_field, db_column in self.FIELD_MAPPING.items():
            value = property_map.get(api_field)
            # Convert empty strings to None
            transformed[db_column] = value if value else None

        return transformed

    def _upsert_lead(self, cursor, lead_data: Dict[str, Optional[str]]) -> None:
        """Upsert a lead into the database.

        Args:
            cursor: Database cursor
            lead_data: Dictionary with column names and values
        """
        columns = list(lead_data.keys())
        values = [lead_data[col] for col in columns]

        # Build the INSERT ... ON CONFLICT query
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # For the ON CONFLICT UPDATE clause, exclude the primary key
        update_columns = [col for col in columns if col != 'prospectid']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        # Add airflow_modified_on to update on conflict
        update_clause += ', airflow_modified_on = CURRENT_TIMESTAMP'

        query = f"""
            INSERT INTO lsq_leads_v2 ({column_names}, airflow_created_on, airflow_modified_on)
            VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (prospectid)
            DO UPDATE SET {update_clause}
        """

        cursor.execute(query, values)


@dag(
        dag_id="fetch_lsq_leads_dag",
        schedule="*/15 * * * *",  # Run every 15 minutes
        start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
        catchup=False,
        max_active_runs=2,
        tags=["lsq", "leads", "data_ingestion"],
        default_args={
                "owner": "data_team",
                "retries": 2,
                "retry_delay": pendulum.duration(minutes=5),
        },
        params={
                "start_time": Param(
                        None,
                        type=["null", "string"],
                        description="Start time in format 'YYYY-MM-DD HH:MM:SS'. If not provided, fetches from 2 hours ago."
                ),
                "end_time": Param(
                        None,
                        type=["null", "string"],
                        description="End time in format 'YYYY-MM-DD HH:MM:SS'. If not provided, uses current time."
                ),
        },
        doc_md="""
    # LeadSquare Leads Ingestion DAG

    This DAG fetches leads from LeadSquare API and stores them in the lsq_leads_v2 table.

    - Runs every 15 minutes
    - By default, fetches leads modified in the last 2 hours
    - Can override time range using start_time and end_time params
    - Uses upsert logic to handle duplicate leads

    ## Parameters:
    - start_time: Optional start datetime (format: 'YYYY-MM-DD HH:MM:SS')
    - end_time: Optional end datetime (format: 'YYYY-MM-DD HH:MM:SS')
    """,
)
def fetch_lsq_leads_dag():
    """DAG for ingesting LSQ leads."""

    @task(task_id="create_table")
    def create_table() -> bool:
        """Create lsq_leads_v2 table if it doesn't exist."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQLeadsManager(pg_hook, lsq_client)
        manager.create_table()

        return True

    @task(task_id="fetch_and_store_leads")
    def fetch_and_store_leads(table_created: bool, **context) -> Dict[str, Any]:
        """Fetch leads from LSQ and store in database."""
        if not table_created:
            raise ValueError("Table creation failed")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get LSQ credentials from Airflow Variables (all caps)
        lsq_host = Variable.get("LSQ_HOST")
        lsq_access_key = Variable.get("LSQ_ACCESS_KEY")
        lsq_secret_key = Variable.get("LSQ_SECRET_KEY")

        lsq_client = LSQClient(lsq_host, lsq_access_key, lsq_secret_key)
        manager = LSQLeadsManager(pg_hook, lsq_client)

        # Get params
        params = context["params"]
        start_time_param = params.get("start_time")
        end_time_param = params.get("end_time")

        # Calculate time range
        if start_time_param and end_time_param:
            # Use provided params
            from_date_str = start_time_param
            to_date_str = end_time_param
            logger.info(f"Using provided time range: {from_date_str} to {to_date_str}")
        else:
            # Default: last 2 hours
            current_time = datetime.now(timezone.utc)
            to_date = current_time.replace(second=0, microsecond=0)
            from_date = to_date - timedelta(hours=2)

            from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
            to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Using default time range (last 2 hours): {from_date_str} to {to_date_str}")

        total_leads = manager.fetch_and_store_leads(from_date_str, to_date_str)

        return {
                "total_leads": total_leads,
                "from_date": from_date_str,
                "to_date": to_date_str
        }

    # Define task dependencies
    table_created = create_table()
    result = fetch_and_store_leads(table_created)

    return result


# Instantiate the DAG
fetch_lsq_leads_dag_instance = fetch_lsq_leads_dag()
