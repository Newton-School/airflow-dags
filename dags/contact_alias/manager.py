import uuid
import logging
from typing import List, Optional, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .sql_queries import (
    TABLE_QUERIES,
    AUTH_USER_QUERIES,
    FORM_RESPONSE_QUERIES,
    CONTACT_ALIAS_QUERIES
)

logger = logging.getLogger(__name__)


class ContactAliasManager:
    """Manager class for contact alias operations."""

    def __init__(
            self,
            result_db_hook: PostgresHook,
            source_db_hook: PostgresHook,
            back_fill: bool = False
    ):
        """Initialize with database connections.

        Args:
            result_db_hook: Hook for the results database
            source_db_hook: Hook for the source database
            back_fill: Whether to back-fill historical data or just process yesterday's data
        """
        self.result_db_hook = result_db_hook
        self.source_db_hook = source_db_hook
        self.back_fill = back_fill
        logger.info(f"Initializing ContactAliasManager with back_fill={back_fill}")

    def create_contact_alias_table(self) -> None:
        """Create the contact aliases table if it doesn't exist."""
        with self.result_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(TABLE_QUERIES["INITIALIZE_TABLE"])
                conn.commit()
        logger.info("Contact alias table created or verified")

    def get_total_count(self, query_type: str, data_source: str) -> int:
        """Get total record count from source database.

        Args:
            query_type: Either 'ALL' or 'YESTERDAY'
            data_source: Either 'AUTH_USER' or 'FORM_RESPONSE'

        Returns:
            Total count of records
        """
        query_map = {
                ('ALL', 'AUTH_USER'): AUTH_USER_QUERIES["TOTAL_COUNT_ALL"],
                ('YESTERDAY', 'AUTH_USER'): AUTH_USER_QUERIES["TOTAL_COUNT_YESTERDAY"],
                ('ALL', 'FORM_RESPONSE'): FORM_RESPONSE_QUERIES["TOTAL_COUNT_ALL"],
                ('YESTERDAY', 'FORM_RESPONSE'): FORM_RESPONSE_QUERIES["TOTAL_COUNT_YESTERDAY"]
        }

        query = query_map.get((query_type, data_source))
        if not query:
            raise ValueError(f"Invalid query type '{query_type}' or data source '{data_source}'")

        with self.source_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0]

    def fetch_batch_data(self, query_type: str, data_source: str, batch_size: int, offset: int) -> List[Tuple]:
        """Fetch a batch of data from source database.

        Args:
            query_type: Either 'ALL' or 'YESTERDAY'
            data_source: Either 'AUTH_USER' or 'FORM_RESPONSE'
            batch_size: Number of records to fetch
            offset: Offset for pagination

        Returns:
            List of data records
        """
        query_map = {
                ('ALL', 'AUTH_USER'): AUTH_USER_QUERIES["FETCH_USER_DATA_ALL"],
                ('YESTERDAY', 'AUTH_USER'): AUTH_USER_QUERIES["FETCH_USER_DATA_YESTERDAY"],
                ('ALL', 'FORM_RESPONSE'): FORM_RESPONSE_QUERIES["FETCH_USER_DATA_ALL"],
                ('YESTERDAY', 'FORM_RESPONSE'): FORM_RESPONSE_QUERIES["FETCH_USER_DATA_YESTERDAY"]
        }

        query = query_map.get((query_type, data_source))
        if not query:
            raise ValueError(f"Invalid query type '{query_type}' or data source '{data_source}'")

        with self.source_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (batch_size, offset))
                return cursor.fetchall()

    def process_contact_data(
            self,
            email: Optional[str],
            phone: Optional[str],
            user_id: Optional[str] = None
    ) -> None:
        """Process contact data and update/insert records in contact_aliases table.

        Args:
            email: Email address
            phone: Phone number
            user_id: Optional user ID (can be None for form responses)
        """
        # Skip if both email and phone are NULL (violates CHECK constraint)
        if email is None and phone is None:
            return

        with self.result_db_hook.get_conn() as conn:
            try:
                # Start a transaction
                conn.autocommit = False
                with conn.cursor() as cursor:
                    # First check for exact match
                    cursor.execute(
                            CONTACT_ALIAS_QUERIES["FETCH_EXACT_MATCH"],
                            (email, phone)
                    )
                    exact_match = cursor.fetchone()

                    if exact_match:
                        # If exact match exists but user_id needs updating
                        if exact_match[1] is None and user_id is not None:
                            cursor.execute(
                                    CONTACT_ALIAS_QUERIES["UPDATE_USER_ID"],
                                    (user_id, exact_match[0])
                            )
                        # Skip the rest as we have an exact match
                        conn.commit()
                        return

                    # Check for related aliases by email or phone
                    cursor.execute(
                            CONTACT_ALIAS_QUERIES["FETCH_BY_EMAIL_OR_PHONE"],
                            (email, phone)
                    )
                    existing_aliases = cursor.fetchall()

                    # Handle identity group assignment
                    if existing_aliases:
                        self._handle_existing_aliases(cursor, existing_aliases, user_id)
                        primary_identity_group_id = existing_aliases[0][1]
                        # Find first non-null user_id among existing aliases
                        user_ids = [alias[2] for alias in existing_aliases if alias[2] is not None]
                        primary_user_id = user_ids[0] if user_ids else user_id
                    else:
                        # Create new identity group
                        primary_identity_group_id = str(uuid.uuid4())
                        primary_user_id = user_id

                    # Insert the new record
                    cursor.execute(
                            CONTACT_ALIAS_QUERIES["INSERT_CONTACT_ALIAS"],
                            (primary_user_id, email, phone, primary_identity_group_id)
                    )
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error processing email={email}, phone={phone}, user_id={user_id}: {str(e)}")
            finally:
                conn.autocommit = True

    def _handle_existing_aliases(
            self,
            cursor,
            existing_aliases: List[Tuple],
            user_id: Optional[str]
    ) -> None:
        """Consolidate identity groups and update user IDs.

        Args:
            cursor: Database cursor
            existing_aliases: List of existing alias records
            user_id: User ID to assign where missing
        """
        # Get primary identity group and user ID
        primary_identity_group_id = existing_aliases[0][1]
        identity_group_ids = list(set(alias[1] for alias in existing_aliases))

        # Consolidate multiple identity groups if needed
        if len(identity_group_ids) > 1:
            secondary_group_ids = identity_group_ids[1:]
            self._consolidate_identity_groups(cursor, primary_identity_group_id, secondary_group_ids)

        # Find first non-null user_id
        user_ids = [alias[2] for alias in existing_aliases if alias[2] is not None]
        primary_user_id = user_ids[0] if user_ids else user_id

        # Only update user_id if we have a valid one to use
        if primary_user_id is not None:
            cursor.execute(
                    CONTACT_ALIAS_QUERIES["UPDATE_USER_ID_FOR_GROUP"],
                    (primary_user_id, primary_identity_group_id)
            )

    def _consolidate_identity_groups(
            self,
            cursor,
            primary_group_id: str,
            secondary_group_ids: List[str]
    ) -> None:
        """Consolidate secondary identity groups into primary one.

        Args:
            cursor: Database cursor
            primary_group_id: Primary identity group ID to keep
            secondary_group_ids: List of secondary identity group IDs to merge
        """
        if not secondary_group_ids:
            return

        if len(secondary_group_ids) == 1:
            # Handle single item case
            cursor.execute(
                    CONTACT_ALIAS_QUERIES["UPDATE_IDENTITY_GROUP"],
                    (primary_group_id, secondary_group_ids[0])
            )
        else:
            # Handle multiple items
            cursor.execute(
                    CONTACT_ALIAS_QUERIES["UPDATE_IDENTITY_GROUP_MULTIPLE"],
                    (primary_group_id, tuple(secondary_group_ids))
            )

    def process_auth_user_data(self) -> None:
        """Process all user data from auth_user table."""
        query_type = "ALL" if self.back_fill else "YESTERDAY"

        total_count = self.get_total_count(query_type, "AUTH_USER")
        logger.info(f"Processing {total_count} records from auth_user table ({query_type} mode)")

        # Skip if no records to process
        if total_count == 0:
            logger.info("No auth user records to process")
            return

        batch_size = 1000
        for offset in range(0, total_count, batch_size):
            batch_data = self.fetch_batch_data(
                    query_type,
                    "AUTH_USER",
                    batch_size,
                    offset
            )
            logger.info(f"Processing batch of {len(batch_data)} records, offset {offset}")

            for user_data in batch_data:
                user_id, email, phone = user_data[0], user_data[1], user_data[2]
                self.process_contact_data(email, phone, user_id)

        logger.info(f"Completed processing auth user data")

    def process_form_response_data(self) -> None:
        """Process all user data from generic form responses."""
        query_type = "ALL" if self.back_fill else "YESTERDAY"

        total_count = self.get_total_count(query_type, "FORM_RESPONSE")
        logger.info(f"Processing {total_count} records from generic form responses ({query_type} mode)")

        # Skip if no records to process
        if total_count == 0:
            logger.info("No form response records to process")
            return

        batch_size = 1000
        for offset in range(0, total_count, batch_size):
            batch_data = self.fetch_batch_data(
                    query_type,
                    "FORM_RESPONSE",
                    batch_size,
                    offset
            )
            logger.info(f"Processing batch of {len(batch_data)} records, offset {offset}")

            for user_data in batch_data:
                email, phone = user_data[0], user_data[1]
                self.process_contact_data(email, phone)

        logger.info(f"Completed processing form response data")
