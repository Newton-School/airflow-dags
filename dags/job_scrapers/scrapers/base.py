from abc import ABC, abstractmethod
from typing import Iterator, List, Dict, Any
import time
from airflow.utils.log.logging_mixin import LoggingMixin

from ..exceptions import ConfigError, JobScrapingError, RateLimitError
from ..models import RawJobOpening


class BaseJobScraper(LoggingMixin, ABC):
    """
    Abstract base class for job board scrapers.
    Each source (Indeed, Weekday, etc.) should implement its own scraper class.
    """

    def __init__(
            self, batch_size: int = 100, max_retries: int = 3, retry_delay: int = 60
    ):
        """
        Initialize the scraper.

        Args:
            batch_size: Number of jobs to fetch per batch
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Base delay in seconds between retries
        """
        super().__init__()
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._initialized = False

    @classmethod
    def from_airflow_variables(cls, *args, **kwargs) -> "BaseJobScraper":
        """Create scraper instance using Airflow variables"""
        return cls(*args, **kwargs)

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return unique identifier for the job source"""
        pass

    @abstractmethod
    def _get_external_job_id(self, raw_job: Dict[str, Any]) -> str:
        """
        Extract external job ID from raw job data.

        Args:
            raw_job: Dictionary containing raw job data from source

        Returns:
            String identifier that uniquely identifies the job in the source system

        Raises:
            ValueError: If external ID cannot be extracted
        """
        pass

    @abstractmethod
    def _get_is_external_for_job_board(self, raw_job: Dict[str, Any]) -> bool:
        """
        Return True if the job opening is external for the job board.
        """
        pass

    @abstractmethod
    def _setup_impl(self) -> None:
        """Set up any necessary resources (API clients, browsers, etc.)"""
        pass

    @abstractmethod
    def _cleanup_impl(self) -> None:
        """Clean up any resources"""
        pass

    @abstractmethod
    def _fetch_batch(self, batch_number: int) -> List[Dict[str, Any]]:
        """
        Fetch a batch of jobs from the source.

        Args:
            batch_number: Current batch number (0-based)

        Returns:
            List of dictionaries containing raw job data

        Raises:
            JobScrapingError: For general scraping errors
            RateLimitError: When rate limits are hit
        """
        pass

    def setup(self) -> None:
        """Initialize the scraper"""
        if self._initialized:
            return

        try:
            self._setup_impl()
            self._initialized = True
        except Exception as e:
            raise ConfigError(
                    f"Failed to initialize {self.source_name} scraper: {str(e)}"
            )

    def cleanup(self) -> None:
        """Clean up resources"""
        if not self._initialized:
            return

        try:
            self._cleanup_impl()
        except Exception as e:
            self.log.error(f"Error during cleanup: {str(e)}")
        finally:
            self._initialized = False

    @staticmethod
    def should_stop_scraping(raw_job_opening: RawJobOpening) -> bool:
        """
        Given the last job fetched, determine if we should stop scraping.
        """
        return False

    def get_jobs(self) -> Iterator[List[RawJobOpening]]:
        """
        Fetch and yield batches of jobs from the source.

        Yields:
            Lists of RawJobOpening objects

        Raises:
            ConfigError: If scraper is not initialized
            JobScrapingError: For unrecoverable scraping errors
        """
        if not self._initialized:
            self.setup()

        batch_number = 0
        try:
            while True:
                raw_batch = self._fetch_batch_with_retry(batch_number)
                if not raw_batch:  # Empty batch indicates end of data
                    break

                job_openings = []
                for raw_job in raw_batch:
                    try:
                        external_job_id = self._get_external_job_id(raw_job)
                        is_external_for_job_board = self._get_is_external_for_job_board(raw_job)
                        job = RawJobOpening(
                                external_job_id=external_job_id,
                                source_name=self.source_name,
                                raw_data=raw_job,
                                is_external_for_job_board=is_external_for_job_board
                        )
                        job_openings.append(job)
                    except (KeyError, ValueError) as e:
                        self.log.error(f"Invalid job data: {str(e)}")
                        continue

                if job_openings:
                    yield job_openings

                if self.should_stop_scraping(job_openings[-1]):
                    break

                batch_number += 1

        except JobScrapingError as e:
            self.log.error(f"Failed to fetch batch {batch_number}: {str(e)}")
            raise
        finally:
            self.cleanup()

    def _fetch_batch_with_retry(self, batch_number: int) -> List[Dict[str, Any]]:
        """
        Fetch a batch with retry logic.

        Args:
            batch_number: Current batch number

        Returns:
            List of raw job dictionaries

        Raises:
            JobScrapingError: When max retries are exceeded
        """
        last_error = None

        for attempt in range(self._max_retries + 1):
            try:
                return self._fetch_batch(batch_number)

            except RateLimitError as e:
                self.log.warning(f"Rate limit hit on attempt {attempt + 1}")
                last_error = e
                if attempt < self._max_retries:
                    self._handle_rate_limit(attempt)

            except Exception as e:
                self.log.error(
                        f"Error fetching batch on attempt {attempt + 1}: {str(e)}"
                )
                last_error = e
                if attempt < self._max_retries:
                    self._exponential_backoff(attempt)

        raise JobScrapingError(
                f"Max retries ({self._max_retries}) exceeded"
        ) from last_error

    def _handle_rate_limit(self, attempt: int) -> None:
        """Handle rate limit with exponential backoff"""
        delay = self._retry_delay * (2 ** attempt)
        self.log.info(f"Rate limit backoff: {delay} seconds")
        time.sleep(delay)

    def _exponential_backoff(self, attempt: int) -> None:
        """Implement exponential backoff for retries"""
        delay = min(300, self._retry_delay * (2 ** attempt))  # Cap at 5 minutes
        time.sleep(delay)
