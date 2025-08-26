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

        data = response.json()
        if not data[0]:
            return True

        has_expired = data[0].get("data", {}).get("jobview", {}).get("header", {}).get("expired", False)

        return has_expired
