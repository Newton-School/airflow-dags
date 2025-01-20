from abc import ABC, abstractmethod
import time
from typing import Iterator
from airflow.utils.log.logging_mixin import LoggingMixin

from ..managers import CompanyManager
from ..models import RawJobOpening, ProcessedJobListing, Company


class BaseJobTransformer(LoggingMixin, ABC):
    """
    Abstract base class for job data transformers.
    Each source (Indeed, Weekday, etc.) should implement its own transformer.
    """

    def __init__(self, max_retries: int = 3):
        super().__init__()
        self._max_retries = max_retries
        self.company_manager = CompanyManager(self.source)

    @classmethod
    def from_airflow_variables(cls, *args, **kwargs) -> "BaseJobTransformer":
        """Create transformer instance using Airflow variables"""
        return cls(*args, **kwargs)

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return unique identifier for the job source"""
        pass

    @property
    @abstractmethod
    def source(self) -> int:
        """Return unique identifier for the job source as specified in newton-api"""
        pass

    @abstractmethod
    def _extract_company(self, raw_job: RawJobOpening) -> Company:
        """Extract company from raw job data"""
        pass

    @abstractmethod
    def _process_job(
            self, raw_job: RawJobOpening, company_slug: str
    ) -> ProcessedJobListing:
        """
        Transform raw job into processed format after company is resolved.
        Must be implemented by each source.
        """
        pass

    def transform_jobs(
            self, raw_jobs: Iterator[RawJobOpening]
    ) -> Iterator[ProcessedJobListing]:
        """
        Transform a batch of raw jobs into processed jobs.
        Handles company matching and retries.
        """
        for raw_job in raw_jobs:
            for attempt in range(self._max_retries):
                try:
                    company = self._extract_company(raw_job)
                    company_slug = self.company_manager.get_or_create_company_slug(company)

                    processed_job = self._process_job(
                            raw_job, company_slug
                    )
                    yield processed_job
                    break

                except Exception as e:
                    if attempt == self._max_retries - 1:
                        self.log.error(
                                f"Failed to transform job after {self._max_retries} attempts: {str(e)}"
                        )
                    else:
                        self.log.warning(
                                f"Failed to transform job. Retrying attempt {attempt + 1}: {str(e)}"
                        )
                        time.sleep(1)
