import requests
from urllib.parse import urljoin

class WeekdayJobPruner:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
                "accept": "*/*",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }

    def should_prune(self, external_apply_link: str):
        self.session.headers["origin"] = urljoin(external_apply_link, "/")
        response = self.session.get(external_apply_link, timeout=10)

        return response.status_code != 200
