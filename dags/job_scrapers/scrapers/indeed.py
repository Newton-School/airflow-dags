from typing import Dict, Any, List
from dataclasses import dataclass
from airflow.models import Variable
import requests
import json
import re
import time

from .base import BaseJobScraper, RateLimitError, JobScrapingError


@dataclass
class IndeedScraperConfig:
    """Configuration for Indeed job scraper loaded from Airflow variables"""

    # Authentication
    ctk_token: str
    csrf_token: str
    cf_bm: str
    cfuvid: str

    # Search parameters
    search_query: str
    location: str
    search_filters: str

    # Rate limiting
    request_delay: float

    # Request configuration
    user_agent: str
    timeout: int = 30

    @classmethod
    def from_airflow_variables(cls) -> "IndeedScraperConfig":
        """Create config from Airflow variables"""
        # Get required authentication tokens
        ctk_token = Variable.get("indeed_ctk_token", default_var=None)
        csrf_token = Variable.get("indeed_csrf_token", default_var=None)
        cf_bm = Variable.get("indeed_cf_bm", default_var=None)
        cfuvid = Variable.get("indeed_cfuvid", default_var=None)

        if not (ctk_token and csrf_token and cf_bm and cfuvid):
            raise ValueError("Missing required Indeed authentication tokens")

        # Get search parameters
        try:
            search_params = json.loads(
                    Variable.get("indeed_search_params", default_var="{}")
            )
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in indeed_search_params")

        return cls(
                ctk_token=ctk_token,
                csrf_token=csrf_token,
                cf_bm=cf_bm,
                cfuvid=cfuvid,
                search_query=search_params.get("query", "software"),
                location=search_params.get("location", "India"),
                search_filters=search_params.get("filters", "0kf:attr(VDTG7);"),
                request_delay=float(Variable.get("indeed_rate_limit", default_var="1.0")),
                user_agent=Variable.get(
                        "indeed_user_agent",
                        default_var="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ),
        )

    def get_headers(self) -> Dict[str, str]:
        """Generate base headers for requests"""
        return {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
        }


class IndeedJobScraper(BaseJobScraper):
    """Indeed job board scraper implementation"""

    def __init__(self, config: IndeedScraperConfig, batch_size: int = 10):
        super().__init__(batch_size=batch_size)
        self.config = config
        self._session = None
        self._base_url = "https://in.indeed.com/jobs"

    @classmethod
    def from_airflow_variables(cls, batch_size: int = 10) -> "IndeedJobScraper":
        """Create scraper instance using Airflow variables"""
        config = IndeedScraperConfig.from_airflow_variables()
        return cls(config, batch_size=batch_size)

    @property
    def source(self) -> int:
        return 3

    @property
    def source_name(self) -> str:
        return "indeed"

    def _get_external_job_id(self, raw_job: Dict[str, Any]) -> str:
        """Extract Indeed's job key as external ID"""
        if "jobkey" not in raw_job:
            raise ValueError("Job data missing required field 'jobkey'")
        return raw_job["jobkey"]

    def _get_is_external_for_job_board(self, raw_job: Dict[str, Any]) -> bool:
        return True  # TODO: Implement this

    def _generate_timestamp_cookies(self) -> str:
        """Generate timestamp-based cookies for authentication"""
        current_epoch = str(int(time.time() * 1000))

        cookies = {
                "CTK": self.config.ctk_token,
                "INDEED_CSRF_TOKEN": self.config.csrf_token,
                "cf_bm": self.config.cf_bm.format(epoch=current_epoch),
                "cfu": self.config.cfuvid.format(epoch=current_epoch),
                "LV": f'"LA={current_epoch}:CV={current_epoch}:TS={current_epoch}"',
                "PREF": f'"TM={current_epoch}:L={self.config.location}"',
                "RQ": f'"q={self.config.search_query}&l={self.config.location}&ts={current_epoch}"',
        }

        return "; ".join([f"{key}={value}" for key, value in cookies.items()])

    def _setup_impl(self) -> None:
        """Initialize requests session with required headers"""
        self._session = requests.Session()
        self._session.headers.update(self.config.get_headers())

    def _cleanup_impl(self) -> None:
        """Clean up requests session"""
        if self._session:
            self._session.close()
            self._session = None

    def _build_search_params(self, start_value: int) -> Dict[str, Any]:
        """Build search parameters for Indeed query"""
        return {
                "q": self.config.search_query,
                "l": self.config.location,
                "start": start_value,
                "sc": self.config.search_filters,
        }

    def _extract_jobs_from_response(self, html_content: str) -> List[Dict[str, Any]]:
        """Extract job data from HTML response"""
        pattern = (
                r'window\.mosaic\.providerData\["mosaic-provider-jobcards"\]\s*=\s*({.*?});'
        )
        match = re.search(pattern, html_content, re.DOTALL)

        if not match:
            return []

        try:
            json_data = json.loads(match.group(1))
            return (
                    json_data.get("metaData", {})
                    .get("mosaicProviderJobCardsModel", {})
                    .get("results", [])
            )
        except json.JSONDecodeError as e:
            raise JobScrapingError(f"Failed to parse job data: {str(e)}")

    def _fetch_batch(self, batch_number: int) -> List[Dict[str, Any]]:
        """Fetch a batch of jobs from Indeed"""
        if not self._session:
            raise JobScrapingError("Scraper not properly initialized")

        try:
            # Calculate start value
            start_value = batch_number * self._batch_size

            # Update cookies for this request
            self._session.headers.update({"Cookie": self._generate_timestamp_cookies()})

            # Build URL with parameters
            params = self._build_search_params(start_value)
            url = f"{self._base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

            # Make request with configured delay
            time.sleep(self.config.request_delay)
            response = self._session.get(url, timeout=self.config.timeout)
            response.encoding = "utf-8"

            if response.status_code == 429:
                raise RateLimitError("Indeed rate limit reached")

            response.raise_for_status()

            return self._extract_jobs_from_response(response.text)

        except requests.exceptions.RequestException as e:
            raise JobScrapingError(f"Request failed: {str(e)}")
        except Exception as e:
            raise JobScrapingError(f"Unexpected error: {str(e)}")
