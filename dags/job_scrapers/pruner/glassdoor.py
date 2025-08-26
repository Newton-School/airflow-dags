import requests

from ..scrapers.glassdoor import GlassdoorScraperConfig


class GlassdoorJobPruner:
    def __init__(self):
        scarper_config = GlassdoorScraperConfig.from_airflow_variables()
        self.session = requests.Session()
        self.session.headers = scarper_config.get_headers()

    def should_prune(self, external_job_id: str):
        payload = [
                {
                        "operationName": "JobDetailQuery",
                        "variables": {
                                "jl": external_job_id
                        },
                        "query": "query JobDetailQuery($jl: Long!) { jobview: jobView(listingId: $jl) { header { "
                                 "expired } } }"
                }
        ]

        response = self.session.post("https://www.glassdoor.co.in/graph", json=payload)

        response.raise_for_status()

        try:
            data = response.json()
        except Exception as e:
            print(f"Error parsing JSON response for job {external_job_id}: {str(e)}")
            return True

        if not data or not isinstance(data, list) or len(data) == 0:
            print(f"Invalid response structure for job {external_job_id}: {data}")
            return True

        first_item = data[0]
        if first_item is None:
            print(f"First item is None for job {external_job_id}")
            return True

        job_data = first_item.get("data")
        if not job_data:
            print(f"No data field for job {external_job_id}: {first_item}")
            return True

        jobview = job_data.get("jobview")
        if not jobview:
            print(f"No jobview field for job {external_job_id}: {job_data}")
            return True

        header = jobview.get("header")
        if not header:
            print(f"No header field for job {external_job_id}: {jobview}")
            return True

        has_expired = header.get("expired", False)


        return has_expired
