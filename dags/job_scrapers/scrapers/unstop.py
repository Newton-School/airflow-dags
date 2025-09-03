import json
import re
from dataclasses import dataclass, field
from typing import Dict, Any, Iterator, List, Tuple, Optional

import requests
from airflow.models import Variable

from .base import BaseJobScraper
from ..exceptions import RateLimitError
from ..models import RawJobOpening, EmploymentType
from ..utils import generate_random_string


@dataclass
class UnstopScraperConfig:
    api_base_url : str = "https://unstop.com/api/public/"
    filter_params : Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_airflow_variables(cls) -> 'UnstopScraperConfig':
        try:
            unstop_internship_filter_params = Variable.get("unstop_internship_filter_params", default_var={})
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in unstop_internship_filter_params")

        return cls(
                filter_params = unstop_internship_filter_params,
                api_base_url = cls.api_base_url
        )

    def get_headers(self) -> Dict[str, str]:
        return {
                  "accept": "application/json, text/plain, */*",
                  "accept-encoding": "gzip, deflate, br, zstd",
                  "accept-language": "en-US,en;q=0.9",
                  "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
                  "sec-ch-ua-mobile": "?0",
                  "sec-ch-ua-platform": "\"macOS\"",
                  "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                  "cookie": "country=IN",
                  "connection": "keep-alive",
                  "sec-fetch-dest": "empty",
                  "sec-fetch-mode": "cors",
                  "sec-fetch-site": "same-origin"
        }


class UnstopJobScraper(BaseJobScraper):

    def __init__(self, config : UnstopScraperConfig, batch_size: int = 10, max_batch_count: int = 15):
        super().__init__(batch_size=batch_size)
        self.config = config
        self.batch_size = batch_size
        self._session = requests.Session()
        self._session.headers.update(self.config.get_headers())
        self.max_batch_count = max_batch_count

    @classmethod
    def from_airflow_variables(cls, batch_size: int = 100) -> "UnstopJobScraper":
        config = UnstopScraperConfig.from_airflow_variables()
        return cls(config, batch_size=batch_size)

    @property
    def source(self) -> int:
        return 5

    @property
    def source_name(self) -> str:
        return "unstop"

    def _get_external_job_id(self, raw_job: Dict[str, Any]) -> str:
        """Extract Unstop's job key as external ID"""
        job_id = raw_job.get("id")
        if not job_id:
            raise ValueError("Missing job ID in raw job data")
        return job_id


    def _get_is_external_for_job_board(self, raw_job: Dict[str, Any]) -> bool:
        """Check if the job is external (redirects to company site)"""
        regn_open = raw_job.get("regn_open", 1)
        return regn_open == 0

    def _setup_impl(self) -> None:
        """Set up any necessary resources (API clients, browsers, etc.)"""
        self._session = requests.session()
        self._session.headers.update(self.config.get_headers())

    def _cleanup_impl(self) -> None:
        """Clean up any resources"""
        if self._session:
            self._session.close()
            self._session = None

    def _fetch_batch(self, batch_number : int, *args, **kwargs) -> Tuple[List[Dict[str, Any]], bool]:
        filter_params = kwargs.get("filter_params")
        job_type = kwargs.get("job_type", EmploymentType.FULL_TIME.value)

        if job_type == EmploymentType.INTERNSHIP:
            opportunity = "internships"
        else:
            opportunity = "jobs"
        filter_params['opportunity'] = opportunity
        filter_params['page'] = batch_number

        api_base_url = self.config.api_base_url
        response = self._session.get(f'{api_base_url}opportunity/search-result/', params = filter_params)

        if response.status_code == 429:
            raise RateLimitError("Rate limit exceeded")

        response.raise_for_status()

        data = response.json()
        jobs = data.get("data", {}).get("data", [])
        current_page = data.get("data", {}).get("current_page")
        last_page = data.get("data", {}).get("last_page")

        return jobs, (current_page < last_page)


    def get_jobs(self, job_type : Optional[int]) -> Iterator[List[RawJobOpening]]:
        if not self._initialized:
            self.setup()

        if not job_type:
            job_type = EmploymentType.INTERNSHIP.value

        batch_number = 1
        while True:
            raw_batch, has_more = self._fetch_batch_with_retry(
                    batch_number, job_type=job_type
            )
            if not raw_batch:
                break

            job_openings = []
            for raw_job in raw_batch:
                try:
                    external_id = self._get_external_job_id(raw_job)
                    is_external_for_job_board = self._get_is_external_for_job_board(raw_job)
                    raw_data = raw_job
                    raw_data["job_type"] = job_type

                    job = RawJobOpening(
                            external_job_id=external_id,
                            source_name=self.source_name,
                            raw_data=raw_data,
                            is_external_for_job_board=is_external_for_job_board,
                    )
                    job_openings.append(job)
                except (KeyError, ValueError) as e:
                    self.log.error(f"Error processing job: {e}")
                    continue

            if job_openings:
                yield job_openings

            if not has_more:
                break

            batch_number += 1
            if batch_number > self.max_batch_count:
                break
