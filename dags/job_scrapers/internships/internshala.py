import re
import time
import logging
import random
from dataclasses import dataclass
from typing import List, Tuple, Any, Iterator, Optional

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, HTTPError, Timeout


@dataclass
class InternShalaJob:
    title: str
    company: str
    job_id: str
    locations: Optional[List[str]]
    job_description_url: Optional[str]
    min_monthly_stipend: Optional[int]
    max_monthly_stipend: Optional[int]
    required_skills: Optional[List[str]]
    duration: Optional[str]
    start_date: Optional[str]
    apply_by: Optional[str]
    source: str = "InternShala"
    description_raw_text: Optional[str] = None


class InternShalaScrapingError(Exception):
    """Exception raised for InternShala scraping errors."""
    pass


class InternShalaRateLimitError(InternShalaScrapingError):
    """Exception raised when rate limits are hit."""
    pass


class InternShalaInternshipScraper:
    """
    Scraper for InternShala internship listings.
    Fetches and parses internship data from internshala.com.
    """

    def __init__(
            self,
            max_retries: int = 3,
            retry_delay: int = 5,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ):
        self.base_url = "https://internshala.com"
        self.headers = {
                "User-Agent": user_agent,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)

    def build_job_list_url(self, job_role: str, page: int) -> str:
        """
        Build the URL for fetching job listings based on the job role and page number.
        """
        job_role_url_text = job_role.replace(" ", "-").lower()
        job_role_url_text += "-internship"
        return f"{self.base_url}/internships_ajax/{job_role_url_text}/page-{page}"

    @staticmethod
    def get_job_id_from_url(internship_url: str) -> Optional[str]:
        """
        Extract job ID from the internship URL.
        """
        match = re.search(r'(?:internship|job)/detail/[^/]*?(\d+)', internship_url)

        if match:
            job_id = match.group(1)
            return str(job_id)

        return None


    @staticmethod
    def parse_stipend_string(stipend_str: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Parse stipend string to extract minimum and maximum stipend amounts.
        """
        try:
            pattern = r'₹\s?([\d,]+)(?:\s*-\s*(?:₹\s?)?([\d,]+))?'
            match = re.search(pattern, stipend_str)
            if match:
                min_stipend = int(match.group(1).replace(',', ''))
                # If second group is found, use it; otherwise, the max is the same as min.
                max_stipend = int(match.group(2).replace(',', '')) if match.group(2) else min_stipend
                return min_stipend, max_stipend
        except (ValueError, AttributeError) as e:
            logging.warning(f"Failed to parse stipend string '{stipend_str}': {e}")

        return None, None

    def _make_request(self, url: str, method: str = "GET", json_response: bool = False, **kwargs) -> Any:
        """
        Make a request with retry logic.
        """
        for attempt in range(self._max_retries):
            try:
                response = requests.request(
                        method, url, headers=self.headers, timeout=30, **kwargs
                )

                if response.status_code == 429:
                    raise InternShalaRateLimitError("Rate limit exceeded")

                response.raise_for_status()
                return response.json() if json_response else response

            except (RequestException, Timeout) as e:
                delay = self._retry_delay * (2 ** attempt) + random.uniform(0, 1)
                self.logger.info(f"Request failed, retrying in {delay:.2f} seconds: {e}")
                time.sleep(delay)

        # Final attempt
        try:
            response = requests.request(
                    method, url, headers=self.headers, timeout=30, **kwargs
            )

            if response.status_code == 429:
                raise InternShalaRateLimitError("Rate limit exceeded")

            response.raise_for_status()
            return response.json() if json_response else response

        except HTTPError as e:
            if e.response.status_code == 404:
                raise InternShalaScrapingError(f"Resource not found: {url}")
            raise InternShalaScrapingError(f"HTTP error: {str(e)}")
        except Exception as e:
            raise InternShalaScrapingError(f"Request failed: {str(e)}")

    def get_internship_detail(self, internship_url: str) -> InternShalaJob:
        """
        Get detailed information about an internship.
        """
        try:
            full_url = f'{self.base_url}{internship_url}'
            self.logger.debug(f"Fetching internship details from {full_url}")
            response = self._make_request(full_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract internship ID
            job_id = self.get_job_id_from_url(internship_url)
            if not job_id:
                raise InternShalaScrapingError(f"Could not extract job ID from URL: {internship_url}")

            # Extract basic information
            detail_container = soup.find('div', class_='detail_view')
            if not detail_container:
                raise InternShalaScrapingError(f"Could not find detail container at {full_url}")

            company_container = soup.find('div', class_='company')
            if not company_container:
                raise InternShalaScrapingError(f"Could not find company container at {full_url}")

            title = company_container.find('div', class_='profile')
            title = title.text.strip() if title else "Unknown Title"

            company = company_container.find('div', class_='company_name')
            company = company.text.strip() if company else "Unknown Company"

            # Extract location
            location_container = detail_container.find('div', id='location_names')
            locations = []
            if location_container:
                locations = [loc.text.strip() for loc in location_container.find_all('span')]

            # Extract other details
            internship_other_details_container = detail_container.find('div', class_='internship_other_details_container')

            # Start date
            start_date = None
            if internship_other_details_container:
                start_date_container = internship_other_details_container.find('div', id='start-date-first')
                if start_date_container and start_date_container.find('span'):
                    start_date = start_date_container.find('span').text.strip()

            # Apply by date
            apply_by = None
            if internship_other_details_container:
                apply_by_container = internship_other_details_container.find('div', class_='apply_by')
                if apply_by_container:
                    apply_by_body = apply_by_container.find('div', class_='item_body')
                    if apply_by_body:
                        apply_by = apply_by_body.text.strip()

            # Duration
            duration = None
            if internship_other_details_container:
                duration_span = internship_other_details_container.find('span', string='Duration')
                if duration_span and (duration_span_parent := duration_span.find_parent()):
                    duration_body = duration_span_parent.find_next_sibling('div', class_='item_body')
                    if duration_body:
                        duration = duration_body.text.strip()

            # Stipend
            min_monthly_stipend = max_monthly_stipend = None
            stipend_elem = detail_container.find('span', class_='stipend')
            if stipend_elem:
                stipend_string = stipend_elem.text.strip()
                min_monthly_stipend, max_monthly_stipend = self.parse_stipend_string(stipend_string)

            # Skills
            required_skills = []
            skills_heading_div = detail_container.find('h3', 'skills_heading')
            if skills_heading_div:
                skills_container = skills_heading_div.find_next_sibling('div')
                if skills_container:
                    required_skills = [skill.text.strip() for skill in skills_container.find_all('span')]

            # Create and return the job object
            return InternShalaJob(
                    job_id=job_id,
                    title=title,
                    company=company,
                    locations=locations,
                    job_description_url=full_url,
                    min_monthly_stipend=min_monthly_stipend,
                    max_monthly_stipend=max_monthly_stipend,
                    required_skills=required_skills,
                    duration=duration,
                    start_date=start_date,
                    apply_by=apply_by
            )

        except InternShalaScrapingError:
            raise
        except Exception as e:
            self.logger.error(f"Error parsing internship details: {e}")
            raise InternShalaScrapingError(f"Failed to parse internship details: {e}")

    def fetch_internships_page(self, job_role: str, page: int) -> List[InternShalaJob]:
        """
        Fetch internships for a specific job role and page.

        """
        url = self.build_job_list_url(job_role, page)
        self.logger.info(f"Fetching internships for {job_role}, page {page}")

        try:
            response = self._make_request(url, json_response=True)

            internship_list_html = response.get('internship_list_html', '')
            current_page_count = response.get('currentPageCount', 0)

            if not internship_list_html or current_page_count == 0:
                self.logger.info("No internships found on this page")
                return []

            soup = BeautifulSoup(internship_list_html, 'html.parser')
            internship_urls = soup.find_all('a', class_='job-title-href')

            internships = []
            for internship_url_elem in internship_urls:
                internship_url = internship_url_elem.get('href')
                if not internship_url:
                    continue

                try:
                    internship = self.get_internship_detail(internship_url)
                    internships.append(internship)
                    self.logger.debug(f"Successfully fetched details for {internship.title}")
                except InternShalaScrapingError as e:
                    self.logger.error(f"Error fetching internship at {internship_url}: {e}")

            return internships

        except InternShalaScrapingError:
            raise
        except Exception as e:
            self.logger.error(f"Error fetching internships list: {e}")
            raise InternShalaScrapingError(f"Failed to fetch internships: {e}")

    def fetch_all_internships(self, job_role: str, max_pages: int = 5) -> Iterator[List[InternShalaJob]]:
        """
        Fetch all internships for a job role across multiple pages.
        """
        for page in range(1, max_pages + 1):
            try:
                internships = self.fetch_internships_page(job_role, page)
                if not internships:
                    self.logger.info(f"No more internships found after page {page}")
                    break

                yield internships

            except InternShalaScrapingError as e:
                self.logger.error(f"Failed to fetch page {page}: {e}")
                break
