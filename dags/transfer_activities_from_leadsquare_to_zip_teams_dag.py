from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

import requests
import json

from datetime import datetime, timezone, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 6),
}


def transfer_activities_from_leadsquare_to_zip_teams_nested(**kwargs):
    today_utc = datetime.now(timezone.utc).date()
    ACTIVITY_EVENTS = [21, 22]
    LEAD_SQUARED_ACCESS_KEY = Variable.get(
        "LEAD_SQUARED_ACCESS_KEY"
    )
    LEAD_SQUARED_SECRET_KEY = Variable.get("LEAD_SQUARED_SECRET_KEY")
    LEAD_SQUARED_URL = "https://api-in21.leadsquared.com"
    ZIP_TEAMS_URL = "https://api.zipteams.com/api/v1/client/leadsquared/inbound/events?eventType=LeadActivity_Post_Create"

    for ACTIVITY_EVENT in ACTIVITY_EVENTS:
        print("Started for ACTIVITY_EVENT: ", ACTIVITY_EVENT)
        ACTIVITY_EVENT_NAME = ""
        if ACTIVITY_EVENT == 22:
            ACTIVITY_EVENT_NAME = "Outbound Phone Call Activity"
        elif ACTIVITY_EVENT == 21:
            ACTIVITY_EVENT_NAME = "Inbound Phone Call Activity"
        else:
            break
        url = f"{LEAD_SQUARED_URL}/v2/ProspectActivity.svc/Activity/Retrieve/BySearchParameter?accessKey={LEAD_SQUARED_ACCESS_KEY}&secretKey={LEAD_SQUARED_SECRET_KEY}"
        advance_search = "{\"GrpConOp\":\"And\",\"Conditions\":[{\"Type\":\"Activity\",\"ConOp\":\"and\",\"RowCondition\":[{\"SubConOp\":\"And\",\"LSO\":\"ActivityEvent\",\"LSO_Type\":\"PAEvent\",\"Operator\":\"eq\",\"RSO\":\""+str(ACTIVITY_EVENT)+"\"},{\"SubConOp\":\"And\",\"LSO_Type\":\"DateTime\",\"LSO\":\"mx_Custom_2\",\"Operator\":\"between\",\"RSO\":\""+f"{today_utc} TO {today_utc}"+"\",\"RSO_IsMailMerged\":false},{\"SubConOp\":\"And\",\"LSO_Type\":\"DateTime\",\"LSO\":\"ActivityTime\",\"Operator\":\"eq\",\"RSO\":\"\"}]},{\"Type\":\"Activity\",\"ConOp\":\"and\",\"RowCondition\":[{\"SubConOp\":\"And\",\"LSO\":\"ActivityEvent\",\"LSO_Type\":\"PAEvent\",\"Operator\":\"eq\",\"RSO\":\""+str(ACTIVITY_EVENT)+"\"},{\"SubConOp\":\"And\",\"LSO_Type\":\"Dropdown\",\"LSO\":\"Status\",\"Operator\":\"eq\",\"RSO\":\"Answered\",\"RSO_IsMailMerged\":false},{\"SubConOp\":\"And\",\"LSO_Type\":\"DateTime\",\"LSO\":\"ActivityTime\",\"Operator\":\"eq\",\"RSO\":\"\"}]}],\"QueryTimeZone\":\"India Standard Time\"}"
        for index in range(6):
            payload = json.dumps(
                {
                    "ActivityEvent": ACTIVITY_EVENT,
                    "AdvancedSearch": advance_search,
                    "Paging": {"PageIndex": index + 1, "PageSize": 1000},
                    "Sorting": {"ColumnName": "ModifiedOn", "Direction": 1},
                    "Columns": {
                        "Include_CSV": "ProspectActivityId,RelatedProspectId,ActivityEvent,ActivityType,CreatedBy,CreatedOn,Score,Status,mx_Custom_10,Owner,mx_Custom_15,mx_Custom_9,mx_Custom_6,mx_Custom_7,mx_Custom_4,mx_Custom_5,mx_Custom_2,Note,mx_Custom_3,mx_Custom_1"
                    },
                }
            )
            headers = {"Content-Type": "application/json"}
            response = requests.request("POST", url, headers=headers, data=payload)
            if not len(response.json()["List"]):
                break
            for activity in response.json()["List"]:
                modified_on = activity["ModifiedOn"]
                modified_on_datetime = datetime.strptime(
                    modified_on, "%Y-%m-%d %H:%M:%S"
                )
                current_time_utc = datetime.utcnow()
                time_difference = current_time_utc - modified_on_datetime
                if not timedelta(hours=1) >= time_difference >= timedelta(0):
                    print(f"Skipping as modified_on - {modified_on}")
                    continue
                print("Posting for ProspectActivityId: ", activity["ProspectActivityId"], activity["mx_Custom_4"])
                if not activity["mx_Custom_4"]:
                    print(f"Skipping as for ProspectActivityId: {activity["ProspectActivityId"]} mx_Custom_4 is empty")
                    continue
                body_to_post = [
                    {
                        "ProspectActivityId": activity["ProspectActivityId"],
                        "RelatedProspectId": activity["RelatedProspectId"],
                        "ActivityEvent": activity["ActivityEvent"],
                        "ActivityEventName": ACTIVITY_EVENT_NAME,
                        "ActivityType": activity["ActivityType"],
                        "CreatedBy": activity["CreatedBy"],
                        "CreatedOn": activity["CreatedOn"],
                        "Score": activity["Score"],
                        "Data": {
                            "Status": activity["Status"],
                            "mx_Custom_10": activity["mx_Custom_10"],
                            "Owner": activity["Owner"],
                            "mx_Custom_15": activity["mx_Custom_15"],
                            "mx_Custom_9": activity["mx_Custom_9"],
                            "mx_Custom_6": activity["mx_Custom_6"],
                            "mx_Custom_7": activity["mx_Custom_7"],
                            "mx_Custom_4": activity["mx_Custom_4"],
                            "mx_Custom_5": activity["mx_Custom_5"],
                            "mx_Custom_2": activity["mx_Custom_2"],
                            "Note": activity["Note"],
                            "mx_Custom_3": activity["mx_Custom_3"],
                            "mx_Custom_1": activity["mx_Custom_1"],
                        },
                    }
                ]
                zipteam_headers = {
                    "admin-connection": "muskan.kumari@newtonschool.co",
                    "Content-Type": "application/json",
                }
                response = requests.request(
                    "POST", ZIP_TEAMS_URL, headers=zipteam_headers, json=body_to_post
                )
                print(f"Response status: {response.status_code} for ProspectActivityId - {activity["ProspectActivityId"]}")
            print("Finished for index: ", index)
        print("Finished for ACTIVITY_EVENT: ", ACTIVITY_EVENT)


dag = DAG(
    "transfer_activities_from_leadsquare_to_zip_teams",
    default_args=default_args,
    description="transfer activities from leadsquare to zip teams",
    schedule_interval="*/30 * * * *",
    catchup=False,
)

transfer_activities_from_leadsquare_to_zip_teams = PythonOperator(
    task_id="transfer_activities_from_leadsquare_to_zip_teams",
    python_callable=transfer_activities_from_leadsquare_to_zip_teams_nested,
    provide_context=True,
    dag=dag,
)
transfer_activities_from_leadsquare_to_zip_teams
