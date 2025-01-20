from datetime import datetime
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
import requests
import json
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from airflow.models import Variable

from .base import BaseJobScraper, RateLimitError, JobScrapingError
from ..models import RawJobOpening


@dataclass
class WeekdayScraperConfig:
    """Configuration for Weekday job scraper"""

    employment_type: str
    locations: List[str]
    category: str
    clusters: List[str]
    request_aes_key: str  # Payload encryption key
    response_aes_key: str  # Response decryption key
    salt: bytes  # Fixed salt for encryption
    base_url: str = "https://prod3.weekday.technology/jds/fetchJds"
    page_size: int = 100

    @classmethod
    def from_airflow_variables(cls) -> "WeekdayScraperConfig":
        """Create config from Airflow variables"""
        request_aes_key = Variable.get("WEEKDAY_REQUEST_AES_KEY", default_var="")
        response_aes_key = Variable.get("WEEKDAY_RESPONSE_AES_KEY", default_var="")
        salt = b'\xa0a8\x848^\xe7G'

        try:
            search_params = json.loads(
                    Variable.get("WEEKDAY_SEARCH_PARAMS", default_var="{}")
            )
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in weekday_search_params")

        return cls(
                employment_type=search_params.get("employment_type", "full-time"),
                locations=search_params.get("locations", ["India"]),
                category=search_params.get("category", "Engineering"),
                clusters=search_params.get(
                        "clusters",
                        [
                                "frontend",
                                "backend",
                                "fullstack",
                                "data engineer",
                                "data science",
                                "machine learning",
                        ],
                ),
                request_aes_key=request_aes_key,
                response_aes_key=response_aes_key,
                salt=salt,
        )


class WeekdayJobScraper(BaseJobScraper):
    """Weekday job board scraper implementation"""

    def __init__(self, config: WeekdayScraperConfig, batch_size: int = 100):
        super().__init__(batch_size=batch_size)
        self.config = config
        self._session = None

    @classmethod
    def from_airflow_variables(cls, batch_size: int = 100) -> "WeekdayJobScraper":
        """Create scraper instance using Airflow variables"""
        config = WeekdayScraperConfig.from_airflow_variables()
        return cls(config, batch_size=batch_size)

    @property
    def source(self) -> int:
        return 2

    @property
    def source_name(self) -> str:
        return "weekday"

    def _get_external_job_id(self, raw_job: Dict[str, Any]) -> str:
        """Extract Weekday's job ID as external ID"""
        if "jdUid" not in raw_job:
            raise ValueError("Job data missing required field 'jdUid'")
        return raw_job["jdUid"]

    def _get_is_external_for_job_board(self, raw_job: Dict[str, Any]) -> bool:
        return raw_job.get('jobType') == "EXTERNAL"

    def _openssl_key_derivation(
            self, password: str, salt: bytes
    ) -> Tuple[bytes, bytes]:
        """Derive key and IV using OpenSSL's method"""
        key_iv = b""
        previous = b""
        password_bytes = password.encode()

        while len(key_iv) < 48:  # 32 bytes for key, 16 bytes for IV
            previous = hashlib.md5(previous + password_bytes + salt).digest()
            key_iv += previous

        return key_iv[:32], key_iv[32:48]

    def _encrypt_json_payload(self, data: Dict[str, Any]) -> str:
        """Encrypt JSON payload for API request"""
        # Order filters exactly as expected
        filters_order = [
                "employmentType",
                "locations",
                "jdsWithReferrals",
                "onlyShowPartneredCompany",
                "category",
                "cluster",
        ]

        ordered_filters = {}
        for key in filters_order:
            if key in data["filters"]:
                ordered_filters[key] = data["filters"][key]

        ordered_data = {
                "filters": ordered_filters,
                "offset": data["offset"],
                "pageSize": data["pageSize"],
                "sortType": data["sortType"],
        }

        # Convert to JSON bytes
        json_str = json.dumps(ordered_data, separators=(",", ":"))
        json_bytes = json_str.encode("utf-8")

        # Derive key and IV
        key, iv = self._openssl_key_derivation(
                self.config.request_aes_key, self.config.salt
        )

        # Encrypt
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_data = pad(json_bytes, AES.block_size)
        encrypted_data = cipher.encrypt(padded_data)

        # Format output
        output = b"Salted__" + self.config.salt + encrypted_data
        return base64.b64encode(output).decode("utf-8")

    def _decrypt_response(self, encrypted_data: str) -> Dict[str, Any]:
        """Decrypt API response"""
        # Decode base64
        encrypted_bytes = base64.b64decode(encrypted_data)

        if not encrypted_bytes.startswith(b"Salted__"):
            raise ValueError("Invalid response format")

        salt = encrypted_bytes[8:16]
        encrypted_content = encrypted_bytes[16:]

        # Derive key and IV
        key, iv = self._openssl_key_derivation(self.config.response_aes_key, salt)

        # Decrypt
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted_data = cipher.decrypt(encrypted_content)

        # Remove padding
        padding_len = decrypted_data[-1]
        decrypted_data = decrypted_data[:-padding_len]

        # Parse JSON
        return json.loads(decrypted_data.decode("utf-8"))

    def _setup_impl(self) -> None:
        """Initialize requests session"""
        self._session = requests.Session()
        self._session.headers.update(
                {
                        "accept": "*/*",
                        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                        "content-type": "application/json",
                        "origin": "https://jobs.weekday.works",
                        "referer": "https://jobs.weekday.works/",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                }
        )

    def _cleanup_impl(self) -> None:
        """Clean up requests session"""
        if self._session:
            self._session.close()
            self._session = None

    def _fetch_batch(self, batch_number: int, *args, **kwargs) -> List[Dict[str, Any]]:
        """Fetch a batch of jobs from Weekday"""
        if not self._session:
            raise JobScrapingError("Scraper not properly initialized")

        try:
            # Calculate offset
            offset = batch_number * self._batch_size

            # Prepare request payload
            payload = {
                    "filters": {
                            "employmentType": self.config.employment_type,
                            "locations": self.config.locations,
                            "jdsWithReferrals": False,
                            "onlyShowPartneredCompany": False,
                            "category": self.config.category,
                            "cluster": self.config.clusters,
                    },
                    "offset": offset,
                    "pageSize": self.config.page_size,
                    "sortType": "latest",
            }

            # Encrypt payload
            encrypted_payload = self._encrypt_json_payload(payload)

            # Make request
            response = self._session.post(
                    self.config.base_url, json={"payload": encrypted_payload}, timeout=30
            )

            if response.status_code == 429:
                raise RateLimitError("Weekday rate limit reached")

            response.raise_for_status()

            # Decrypt and extract jobs
            decrypted_data = self._decrypt_response(response.text)
            jobs = decrypted_data.get("list", [])

            return jobs if jobs else []

        except requests.exceptions.RequestException as e:
            raise JobScrapingError(f"Request failed: {str(e)}")
        except Exception as e:
            raise JobScrapingError(f"Unexpected error: {str(e)}")

    @staticmethod
    def should_stop_scraping(raw_job_opening: RawJobOpening) -> bool:
        raw_data = raw_job_opening.raw_data
        posted_at = raw_data.get("addedOn")
        job_scraping_limit_in_days = Variable.get("WEEKDAY_JOB_SCRAPING_LIMIT_IN_DAYS", default_var=7)
        try:
            return (datetime.now() - datetime.strptime(posted_at, "%Y-%m-%dT%H:%M:%S.%fZ")).days > int(job_scraping_limit_in_days)
        except Exception as e:
            print(e)
            return False
