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
            user_id: Optional[str] = None,
            source: str = 'unknown'
    ) -> None:
        """Process contact data and update/insert records in contact_aliases table.

        Args:
            email: Email address
            phone: Phone number
            user_id: Optional user ID (can be None for form responses)
            source: Source of the contact data (e.g., 'auth_user', 'form_response')
        """
        # Skip if both email and phone are NULL (violates CHECK constraint)
        if email is None and phone is None:
            return

        with self.result_db_hook.get_conn() as conn:
            try:
                # Start a transaction
                conn.autocommit = False
                with conn.cursor() as cursor:
                    # Case 1: Handle auth_user source with user_id
                    if source == 'auth_user' and user_id is not None:
                        if self._handle_auth_user_update(cursor, user_id, email, phone, source):
                            conn.commit()
                            return

                    # Case 2: Handle exact match on email and phone
                    exact_match = self._check_exact_match(cursor, email, phone)
                    if exact_match:
                        # If source is auth_user, update the source
                        if source == 'auth_user' and user_id is not None:
                            cursor.execute(
                                    CONTACT_ALIAS_QUERIES["UPDATE_SOURCE"],
                                    (source, exact_match[0])
                            )

                            # Update user_id if it was NULL
                            if exact_match[1] is None:
                                cursor.execute(
                                        CONTACT_ALIAS_QUERIES["UPDATE_USER_ID"],
                                        (user_id, exact_match[0])
                                )

                        conn.commit()
                        return

                    # Case 3: Handle new contact or partial matches
                    self._handle_new_contact(cursor, email, phone, user_id, source)
                    conn.commit()

            except Exception as e:
                conn.rollback()
                logger.error(f"Error processing email={email}, phone={phone}, user_id={user_id}, source={source}: {str(e)}")
            finally:
                conn.autocommit = True

    def _handle_auth_user_update(
            self,
            cursor,
            user_id: str,
            email: Optional[str],
            phone: Optional[str],
            source: str
    ) -> bool:
        """Handle the auth_user update use case.

        Args:
            cursor: Database cursor
            user_id: User ID
            email: Email address
            phone: Phone number
            source: Source of the contact data

        Returns:
            bool: True if processing is complete, False if further processing needed
        """
        # Find existing contact with this user_id and source
        cursor.execute(
                CONTACT_ALIAS_QUERIES["FETCH_BY_USER_ID_AND_SOURCE"],
                (user_id, source)
        )
        existing_user_records = cursor.fetchall()

        if not existing_user_records:
            # No matching user_id found, continue with normal processing
            return False

        # Get details of the existing record
        existing_id, existing_email, existing_phone, existing_group_id = existing_user_records[0]

        # Check if contact info has changed
        if email == existing_email and phone == existing_phone:
            # No changes, we're done
            return True

        # Contact info has changed, update the record
        cursor.execute(
                CONTACT_ALIAS_QUERIES["UPDATE_CONTACT_INFO"],
                (email, phone, existing_id)
        )

        # Find all identity groups with the new email or phone
        cursor.execute(
                CONTACT_ALIAS_QUERIES["FETCH_BY_EMAIL_OR_PHONE"],
                (email, phone)
        )
        related_aliases = cursor.fetchall()

        if related_aliases:
            # Get all unique identity group IDs except the existing one
            other_group_ids = set(
                    alias[1] for alias in related_aliases
                    if alias[1] != existing_group_id
            )

            # Consolidate other groups into the current one
            if other_group_ids:
                self._consolidate_identity_groups(cursor, existing_group_id, list(other_group_ids))

                # Update user_id for all records in the consolidated group that have no user_id
                cursor.execute(
                        CONTACT_ALIAS_QUERIES["UPDATE_USER_ID_FOR_GROUP"],
                        (user_id, existing_group_id)
                )

        return True

    def _check_exact_match(self, cursor, email: Optional[str], phone: Optional[str]) -> Optional[Tuple]:
        """Check for an exact match on email and phone.

        Args:
            cursor: Database cursor
            email: Email address
            phone: Phone number

        Returns:
            Optional[Tuple]: Match record if found, None otherwise
        """
        cursor.execute(
                CONTACT_ALIAS_QUERIES["FETCH_EXACT_MATCH"],
                (email, phone)
        )
        return cursor.fetchone()

    def _handle_new_contact(
            self,
            cursor,
            email: Optional[str],
            phone: Optional[str],
            user_id: Optional[str],
            source: str
    ) -> None:
        """Handle processing for a new contact or contacts with partial matches.

        Args:
            cursor: Database cursor
            email: Email address
            phone: Phone number
            user_id: User ID
            source: Source of the contact data
        """
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

        # Insert the new record with source
        cursor.execute(
                CONTACT_ALIAS_QUERIES["INSERT_CONTACT_ALIAS"],
                (primary_user_id, email, phone, primary_identity_group_id, source)
        )

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

        if user_id:
            primary_user_id = user_id
        else:
            # Find first non-null user_id
            user_ids = [alias[2] for alias in existing_aliases if alias[2] is not None]
            primary_user_id = user_ids[0] if user_ids else None

        # Only update user_id if we have a valid one to use
        if primary_user_id:
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
                self.process_contact_data(email, phone, user_id, source='auth_user')

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
                self.process_contact_data(email, phone, source='generic_form_response')

        logger.info(f"Completed processing form response data")
