"""LeadSquare API client."""
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

PAGE_SIZE = 5000


class LSQClient:
    """Simple client for LeadSquare API."""

    def __init__(self, host: str, access_key: str, secret_key: str):
        """Initialize LSQ client with credentials.

        Args:
            host: LeadSquare API host URL
            access_key: API access key
            secret_key: API secret key
        """
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = f"{host}/v2/LeadManagement.svc/Leads.RecentlyModified"

    def fetch_leads(
        self,
        from_date: str,
        to_date: str,
        page_index: int = 1,
        required_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Fetch leads from LeadSquare API.

        Args:
            from_date: Start date in format 'YYYY-MM-DD HH:MM:SS'
            to_date: End date in format 'YYYY-MM-DD HH:MM:SS'
            page_index: Page number for pagination
            required_fields: List of fields to fetch

        Returns:
            API response as dictionary
        """
        if required_fields is None:
            required_fields = self._get_required_fields()

        payload = {
            "Parameter": {
                "FromDate": from_date,
                "ToDate": to_date,
            },
            "Columns": {
                "Include_CSV": ','.join(required_fields),
            },
            "Paging": {
                "PageIndex": page_index,
                "PageSize": PAGE_SIZE
            },
            "Sorting": {
                "ColumnName": "ModifiedOn",
                "Direction": "1"
            }
        }

        response = requests.post(
            url=self.endpoint,
            params={
                "accessKey": self.access_key,
                "secretKey": self.secret_key,
            },
            json=payload,
            timeout=60
        )

        response.raise_for_status()
        return response.json()

    @staticmethod
    def _get_required_fields() -> List[str]:
        """Get list of required fields to fetch from API."""
        return [
            "ProspectID",
            "mx_Bucket",
            "mx_City",
            "mx_College_City",
            "mx_College_Name",
            "CreatedBy",
            "CreatedOn",
            "mx_Current_Interested_Course",
            "mx_Date_Of_Birth",
            "EmailAddress",
            "FirstName",
            "mx_Graduation_Year",
            "mx_Highest_Qualification",
            "mx_Last_Call_Connection_Status",
            "mx_Last_Call_Status",
            "mx_Last_Call_Sub_Status",
            "LastName",
            "LeadAge",
            "LeadNumber",
            "Origin",
            "mx_Lead_Owner",
            "QualityScore01",
            "Score",
            "ProspectStage",
            "mx_status",
            "mx_Substatus",
            "mx_Mid_Funnel_Buckets",
            "mx_Mid_Funnel_Count",
            "ModifiedOn",
            "mx_Network_Id",
            "OwnerIdName",
            "mx_Priority_Status",
            "mx_Product_Graduation_Year",
            "mx_Reactivation_Bucket",
            "mx_Reactivation_Date",
            "mx_Source_Intended_Course",
            "mx_Squadstack_Calling",
            "mx_Squadstack_Qualification_Status",
            "mx_UTM_Campaign",
            "mx_UTM_Medium",
            "mx_UTM_Referer",
            "mx_UTM_Source",
            "mx_Work_Experience",
            "mx_Year_of_Passing_in_Text",
            "mx_RFD_Date",
            "mx_doc_collected",
            "mx_doc_approved",
            "mx_total_fees",
            "mx_total_revenue",
            "mx_cibil_check",
            "mx_ICP",
            "mx_Identifer",
            "mx_Organic_Inbound",
            "mx_Entrance_exam_Marks",
            "mx_Lead_Quality_Grade",
            "mx_Lead_Inherent_Intent",
            "mx_Test_Date_n_Time",
            "mx_Lead_Type",
            "mx_Phoenix_Identifer",
            "mx_Phoenix_Lead_Assigned_Date",
            "mx_Prospect_Status",
            "mx_Reactivation_Source",
            "mx_Lead_Status",
            "mx_PMM_Identifier",
            "OwnerId"
        ]
