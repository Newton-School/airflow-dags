import json
import re
from dataclasses import dataclass
from typing import Dict, Any, Iterator, List, Tuple, Optional

import requests
from airflow.models import Variable

from .base import BaseJobScraper
from ..exceptions import RateLimitError
from ..models import RawJobOpening, EmploymentType
from ..utils import generate_random_string


@dataclass
class JobSearchParameters:
    """Search parameters for Glassdoor job scraper"""
    url_path: str
    page_url: str
    keyword: str
    parameter_url_input: str
    seo_friendly_url_input: str

    @classmethod
    def from_url_path(cls, url_path: str) -> "JobSearchParameters":
        """Create instance from URL path"""
        pattern = r'([a-zA-Z0-9-]+)-([a-zA-Z0-9_,]+)\.htm'
        match = re.match(pattern, url_path)
        job_title = ""
        parameter_url_input = ""

        if match:
            job_title = match.group(1)
            parameter_url_input = match.group(2)

        return cls(
                url_path=url_path,
                page_url=f"https://www.glassdoor.co.in/Job/{url_path}",
                keyword=''.join(job_title.split("-")[:-1]),
                parameter_url_input=parameter_url_input,
                seo_friendly_url_input=job_title,
        )


@dataclass
class GlassdoorScraperConfig:
    """Configuration for Glassdoor job scraper loaded from Airflow variables"""
    csrf_token: str
    relevant_job_role_paths: Dict[str, List[str]]
    query: str = ("query JobSearchResultsQuery($excludeJobListingIds: [Long!], $filterParams: [FilterParams], $keyword: String, "
                  "$locationId: Int, $locationType: LocationTypeEnum, $numJobsToShow: Int!, $originalPageUrl: String, $pageCursor: "
                  "String, $pageNumber: Int, $pageType: PageTypeEnum, $parameterUrlInput: String, $queryString: String, "
                  "$seoFriendlyUrlInput: String, $seoUrl: Boolean) {\n  jobListings(\n    contextHolder: {queryString: $queryString, "
                  "pageTypeEnum: $pageType, searchParams: {excludeJobListingIds: $excludeJobListingIds, keyword: $keyword, "
                  "locationId: $locationId, locationType: $locationType, numPerPage: $numJobsToShow, pageCursor: $pageCursor, "
                  "pageNumber: $pageNumber, filterParams: $filterParams, originalPageUrl: $originalPageUrl, seoFriendlyUrlInput: "
                  "$seoFriendlyUrlInput, parameterUrlInput: $parameterUrlInput, seoUrl: $seoUrl, searchType: SR}}\n  ) {\n    "
                  "companyFilterOptions {\n      id\n      shortName\n      __typename\n    }\n    filterOptions\n    indeedCtk\n    "
                  "jobListings {\n      ...JobView\n      __typename\n    }\n    jobListingSeoLinks {\n      linkItems {\n        "
                  "position\n        url\n        __typename\n      }\n      __typename\n    }\n    jobSearchTrackingKey\n    "
                  "jobsPageSeoData {\n      pageMetaDescription\n      pageTitle\n      __typename\n    }\n    paginationCursors {\n      "
                  "cursor\n      pageNumber\n      __typename\n    }\n    indexablePageForSeo\n    searchResultsMetadata {\n      "
                  "searchCriteria {\n        implicitLocation {\n          id\n          localizedDisplayName\n          type\n          "
                  "__typename\n        }\n        keyword\n        location {\n          id\n          shortName\n          "
                  "localizedShortName\n          localizedDisplayName\n          type\n          __typename\n        }\n        "
                  "__typename\n      }\n      footerVO {\n        countryMenu {\n          childNavigationLinks {\n            id\n       "
                  "     link\n            textKey\n            __typename\n          }\n          __typename\n        }\n        "
                  "__typename\n      }\n      helpCenterDomain\n      helpCenterLocale\n      jobAlert {\n        jobAlertExists\n        "
                  "__typename\n      }\n      jobSerpFaq {\n        questions {\n          answer\n          question\n          "
                  "__typename\n        }\n        __typename\n      }\n      jobSerpJobOutlook {\n        occupation\n        paragraph\n "
                  "       heading\n        __typename\n      }\n      showMachineReadableJobs\n      __typename\n    }\n    "
                  "serpSeoLinksVO {\n      relatedJobTitlesResults\n      searchedJobTitle\n      searchedKeyword\n      "
                  "searchedLocationIdAsString\n      searchedLocationSeoName\n      searchedLocationType\n      topCityIdsToNameResults {"
                  "\n        key\n        value\n        __typename\n      }\n      topEmployerIdsToNameResults {\n        key\n        "
                  "value\n        __typename\n      }\n      topEmployerNameResults\n      topOccupationResults\n      __typename\n    "
                  "}\n    totalJobsCount\n    __typename\n  }\n}\n\nfragment JobView on JobListingSearchResult {\n  jobview {\n    header "
                  "{\n      adOrderId\n      advertiserType\n      ageInDays\n      divisionEmployerName\n      easyApply\n      employer "
                  "{\n        id\n        name\n        shortName\n        __typename\n      }\n      organic\n      "
                  "employerNameFromSearch\n      goc\n      gocConfidence\n      gocId\n      isSponsoredJob\n      isSponsoredEmployer\n "
                  "     jobCountryId\n      indeedJobAttribute {\n      skillsLabel\n      }\n      jobLink\n      jobResultTrackingKey\n "
                  "     normalizedJobTitle\n      applyUrl\n      "
                  "jobTitleText\n      "
                  "locationName\n      locationType\n      locId\n      needsCommission\n      payCurrency\n      payPeriod\n      "
                  "payPeriodAdjustedPay {\n        p10\n        p50\n        p90\n        __typename\n      }\n      rating\n      "
                  "salarySource\n      savedJobId\n      seoJobLink\n      __typename\n    }\n    job {\n      descriptionFragments\n     "
                  " description\n      importConfigId\n      jobTitleId\n      jobTitleText\n      listingId\n      __typename\n    }\n    "
                  "jobListingAdminDetails {\n      cpcVal\n      importConfigId\n      jobListingId\n      jobSourceId\n      "
                  "userEligibleForAdminJobDetails\n      __typename\n    }\n    overview {\n      shortName\n      squareLogoUrl\n      "
                  "name\n      website\n      "
                  "__typename\n    }\n    __typename\n  }\n  __typename\n}\n")

    @classmethod
    def from_airflow_variables(cls) -> "GlassdoorScraperConfig":
        """Create config from Airflow variables"""
        csrf_token = generate_random_string(20)

        try:
            relevant_job_role_paths = json.loads(Variable.get("glassdoor_relevant_job_roles", default_var="{}"))
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in glassdoor_relevant_job_roles")

        return cls(
                csrf_token=csrf_token,
                relevant_job_role_paths=relevant_job_role_paths
        )

    def get_headers(self) -> Dict[str, str]:
        """Get headers for HTTP requests"""
        return {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 "
                              "Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-GB,en;q=0.9",
                "apollographql-client-name": "job-search-next",
                "apollographql-client-version": "7.15.4",
                "Content-Type": "application/json",
                "dnt": "1",
                "gd-csrf-token": self.csrf_token,
                "origin": "https://www.glassdoor.co.in",
                "referer": "https://www.glassdoor.co.in/"
        }


class GlassdoorJobScraper(BaseJobScraper):
    """Job scraper for Glassdoor"""

    def __init__(self, config: GlassdoorScraperConfig, batch_size: int = 100, max_batch_count: int = 5):
        super().__init__(batch_size=batch_size)
        self.config = config
        self._session = None
        self._base_url = "https://www.glassdoor.co.in/graph"
        self.max_batch_count = max_batch_count

    @classmethod
    def from_airflow_variables(cls, batch_size: int = 100) -> "GlassdoorJobScraper":
        """Create scraper instance using Airflow variables"""
        config = GlassdoorScraperConfig.from_airflow_variables()
        return cls(config, batch_size=batch_size)

    @property
    def source(self) -> int:
        return 4

    @property
    def source_name(self) -> str:
        return "glassdoor"

    def _get_external_job_id(self, raw_job: Dict[str, Any]) -> str:
        """Extract Glassdoor's job key as external ID"""
        listing_id = raw_job.get("jobview", {}).get("job", {}).get("listingId")

        if not listing_id:
            raise ValueError("Job listing ID not found in raw job data")

        return listing_id

    def _get_is_external_for_job_board(self, raw_job: Dict[str, Any]) -> bool:
        return not raw_job.get("jobview", {}).get("header", {}).get("easyApply", False)

    def _setup_impl(self) -> None:
        """Set up any necessary resources (API clients, browsers, etc.)"""
        self._session = requests.session()
        self._session.headers.update(self.config.get_headers())

    def _cleanup_impl(self) -> None:
        """Clean up any resources"""
        if self._session:
            self._session.close()
            self._session = None

    def _fetch_batch(self, batch_number: int, *args, **kwargs) -> Tuple[List[Dict[str, Any]], str]:
        cursor = kwargs.get("cursor", "")
        search_params = kwargs.get("search_params")
        job_type = kwargs.get("job_type", EmploymentType.FULL_TIME.value)

        if job_type == EmploymentType.INTERNSHIP.value:
            job_type_indeed = "VDTG7"
        else:
            job_type_indeed = "CF3CP"

        payload = [
                {
                        "operationName": "JobSearchResultsQuery",
                        "variables": {
                                "keyword": search_params.keyword,
                                "locationId": 0,
                                "numJobsToShow": 100,
                                "originalPageUrl": search_params.page_url,
                                "parameterUrlInput": search_params.parameter_url_input,
                                "pageType": "SERP",
                                "queryString": "",
                                "seoFriendlyUrlInput": search_params.seo_friendly_url_input,
                                "seoUrl": True,
                                "pageCursor": cursor,
                                "filterParams": [
                                        {"filterKey": "maxSalary", "values": "1999000"},
                                        {"filterKey": "minSalary", "values": "3000"},
                                        {"filterKey": "fromAge", "values": "3"},
                                        {"filterKey": "jobTypeIndeed", "values": job_type_indeed},
                                        {"filterKey": "sortBy", "values": "date_desc"}
                                ]
                        },
                        "query": self.config.query
                }
        ]

        response = self._session.post(self._base_url, json=payload)

        if response.status_code == 429:
            raise RateLimitError("Glassdoor rate limit reached")

        response.raise_for_status()

        data = response.json()
        cursor = ""
        cursors = data[0].get("data", {}).get("jobListings", {}).get("paginationCursors", [])
        # find if cursor exist for next page
        for c in cursors:
            if c.get("pageNumber") == batch_number + 2:
                cursor = c.get("cursor")
                break
        return data[0].get("data", {}).get("jobListings", {}).get("jobListings", []), cursor

    def get_jobs(self, job_type : Optional[int]) -> Iterator[List[RawJobOpening]]:
        if not self._initialized:
            self.setup()

        if not job_type:
            job_type = EmploymentType.FULL_TIME
        for job_role, paths in self.config.relevant_job_role_paths.items():
            for url_path in paths:
                search_params = JobSearchParameters.from_url_path(url_path)
                cursor = ""
                batch_number = 0
                while True:
                    raw_batch, cursor = self._fetch_batch_with_retry(batch_number, search_params=search_params, cursor=cursor, job_type=job_type)
                    if not raw_batch:
                        break

                    job_openings = []
                    for raw_job in raw_batch:
                        try:
                            external_id = self._get_external_job_id(raw_job)
                            is_external_for_job_board = self._get_is_external_for_job_board(raw_job)
                            job = RawJobOpening(
                                    external_job_id=external_id,
                                    source_name=self.source_name,
                                    raw_data=raw_job.get("jobview", {}),
                                    is_external_for_job_board=is_external_for_job_board,
                            )
                            job_openings.append(job)
                        except (KeyError, ValueError) as e:
                            self.log.error(f"Error processing job: {e}")
                            continue

                    if job_openings:
                        yield job_openings

                    if not cursor:
                        break

                    batch_number += 1
                    if batch_number >= self.max_batch_count:
                        break
