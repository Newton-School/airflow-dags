from dataclasses import dataclass
from typing import Optional, List

import requests
from airflow.models import Variable
from openai import OpenAI

from .base import BaseJobTransformer
from .constants import JOB_DESCRIPTION_FORMAT
from .schema import JobDetails
from ..exceptions import RateLimitError
from ..models import RawJobOpening, Company, ProcessedJobListing
from ..scrapers.unstop import UnstopScraperConfig


@dataclass
class UnstopJobMetaData:
    description : str
    apply_url : str
    min_salary : Optional[float] = None
    max_salary : Optional[float] = None

class UnstopJobTransformer(BaseJobTransformer):
    """Transformer for Unstop job data"""

    def __init__(self, max_retries: int = 3):
        super().__init__(max_retries)
        self.openai_client = OpenAI(api_key=Variable.get("OPENAI_API_KEY"))
        scarper_config = UnstopScraperConfig.from_airflow_variables()
        self.session = requests.Session()
        self.session.headers = scarper_config.get_headers()



    @property
    def source(self) -> int:
        return 5

    @property
    def source_name(self) -> str:
        return "unstop"

    def _extract_meta_data(self,raw_job:RawJobOpening) -> UnstopJobMetaData:
        job_id = raw_job.get('external_job_id')

        response = self.session.get(f'{self.scraper_config.api_base_url}competition/{job_id}/?round_lang=1')
        if response.status_code == 429:
            raise RateLimitError("Rate limit exceeded")

        response.raise_for_status()
        data = response.json()
        competition = data.get('competition', {})
        job_description = competition.get('details', '')
        external_apply_link = competition.get('regn_url', '')
        job_detail = competition.get('job_detail', {})
        min_salary = job_detail.get('min_salary', None)
        max_salary = job_detail.get('max_salary',None)
        return UnstopJobMetaData(description=job_description, apply_url=external_apply_link, min_salary = min_salary, max_salary=max_salary)


    def _extract_job_details_ai(self, raw_job: RawJobOpening, job_meta_data: UnstopJobMetaData) -> JobDetails:
        job_description = job_meta_data.description
        min_ctc = job_meta_data.min_salary
        max_ctc = job_meta_data.max_salary
        job_role = raw_job.get('title',None)
        locations = raw_job.get('jobDetail', {}).get('locations', [])
        popular_job_locations = Variable.get('POPULAR_JOB_LOCATIONS', default_var=None)
        existing_job_roles = Variable.get('JOB_ROLES', default_var=None)

        system_prompt = (
            "Extract the job details accurately from the given information.\n"
            "If the information is not present directly, it might be present in job description. Get it from there in that case\n"
            "For job role hash, you need to map the job role to the hash in the given data. You are given names of roles and "
            "their hashes in the variable existing_job_roles. Use this information to map the CLOSEST job role to the hash."
            "Mapping of the job role is compulsory\n"
            "Do not make up any data. If any data is missing, just fill it's value as None.\n"
            "You are also given a list of popular cities and states. Use the specified values for city and state if the given location "
            "matches any of them.\n"
            "For CTC, give the annual CTC response in INR. For example CTC of Rs. 5 Lakhs will be represented as 500000\n"
            "If CTC is in dollars, convert it to INR using the conversion 1 USD = 80 INR. Be careful with number of zeroes while "
            "reporting the CTC. Always double check it.\n"
            "CTC value won't be more than 100000000 in any case\n"
            "Here is the format of the job description required\n"
            f"```\n{JOB_DESCRIPTION_FORMAT}\n{job_description}\n``` In case anything is not available then skip the title itself."
            "Give the job description as very beautiful simple html, with each heading as h4 with font weight 500 and black color. "
            "Don't do too much formatting. Also remove all the links from the description. Keep the description withing 1000 words."
            "There should be no extra spacing within the paragraphs and headings should look like heading. Add a ':' after it\n"
            f"\n```Job description: {job_description}```\n"
            f"Existing Job Roles: {existing_job_roles}\n"
            f"Popular locations: {popular_job_locations}\n"
            "<Job Details>\n"
            f"Job Role: {job_role}\n"
            f"Max CTC: {max_ctc}\n"
            f"Min CTC: {min_ctc}\n"
            f"Location: {locations}\n"
            f"</Job Details>"
        )

        completion = self.openai_client.beta.chat.completions.parse(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                ],
                response_format=JobDetails
        )

        return completion.choices[0].message.parsed


    def _extract_company(self, raw_job: RawJobOpening) -> Company:
        raw_job_opening_data = raw_job.raw_data
        company_info = raw_job_opening_data.get('organisation', {})
        company_name = company_info.get('name', None)
        company_logo_url = company_info.get('logoUrl', None)
        normalized_company_names = [self.company_manager._normalize_name(company_name)]
        company_slug = normalized_company_names[0].replace(" ", "-").lower()

        return Company(
                slug=company_slug,
                name=company_name,
                logo_url=company_logo_url,
        )

    def _process_job(
            self, raw_job : RawJobOpening, company_slug:str
    ) -> ProcessedJobListing:
        raw_job_opening_data = raw_job.raw_data
        title = raw_job_opening_data.get('title')
        external_job_id = raw_job_opening_data.get('id')
        meta_data = self._extract_meta_data(raw_job)
        job_details = self._extract_job_details_ai(raw_job, meta_data)

        skills = []
        if job_details.skills:
            skills = [skill.replace("\x00", "") for skill in job_details.skills if skill]

        return ProcessedJobListing(
                external_job_id=external_job_id.replace("\x00", ""),
                title=title.replace("\x00", ""),
                company_slug=company_slug.replace("\x00", ""),
                role_hash=job_details.job_role_hash.replace("\x00", ""),
                description=job_details.job_description.replace("\x00", ""),
                min_ctc=job_details.min_ctc or 0,
                max_ctc=job_details.max_ctc or 0,
                external_apply_link=meta_data.apply_url,
                city=job_details.city.replace("\x00", ""),
                state=job_details.state.replace("\x00", ""),
                employment_type=raw_job_opening_data.get('job_type', 1),
                skills=skills
        )
