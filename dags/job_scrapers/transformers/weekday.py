from typing import Optional, List

from airflow.models import Variable
from pydantic import BaseModel
from openai import OpenAI

from .base import BaseJobTransformer
from ..models import RawJobOpening, Company, ProcessedJobListing


# Schema for openai
class JobDetails(BaseModel):
    """
    Represents details of a job opening.

    Fields:
    - job_role_hash: (str) Hash of the job role. This cannot be empty
    - max_ctc: (Optional[int]) Maximum CTC offered by the company in INR lakhs.
    - min_ctc: (Optional[int]) Minimum CTC offered by the company in INR lakhs.
    - city: (Optional[str]) City where the job is located.
    - state: (Optional[str]) State where the job is located.
    - skills: List[str]: List of skills required for the job.
    """

    job_role_hash: str
    max_ctc: Optional[int]
    min_ctc: Optional[int]
    city: Optional[str]
    state: Optional[str]
    skills: List[str]


class WeekdayJobTransformer(BaseJobTransformer):
    """Transformer for Weekday job data"""

    def __init__(self, max_retries: int = 3):
        super().__init__(max_retries)
        self.openai_client = OpenAI(api_key=Variable.get("OPENAI_API_KEY"))

    @property
    def source(self) -> int:
        return 2

    @property
    def source_name(self) -> str:
        return "weekday"

    def _extract_company(self, raw_job: RawJobOpening) -> Company:
        raw_job_opening_data = raw_job.raw_data
        company_name = raw_job_opening_data.get("companyName")
        company_description = raw_job_opening_data.get("aboutCompany")
        normalized_company_names = [self.company_manager._normalize_name(company_name)]
        company_slug = normalized_company_names[0].replace(" ", "-").lower()
        company_website = raw_job_opening_data.get("companyWebsite")
        company_logo_url = raw_job_opening_data.get("companyLogo")

        return Company(
            slug=company_slug,
            name=company_name,
            normalized_names=normalized_company_names,
            website=company_website,
            description=company_description,
            logo_url=company_logo_url
        )

    def _extract_job_details_ai(self, raw_job: RawJobOpening) -> JobDetails:
        raw_job_opening_data = raw_job.raw_data
        max_ctc = raw_job_opening_data.get("maxJdSalary")
        min_ctc = raw_job_opening_data.get("minJdSalary")
        ctc_currency = raw_job_opening_data.get("salaryCurrencyCode")
        location = raw_job_opening_data.get("location")
        skills = raw_job_opening_data.get("skills")
        role = raw_job_opening_data.get("role")
        existing_job_roles = Variable.get('JOB_ROLES', default_var=None)

        system_prompt = (
                "Extract the job details accurately from the given information.\n"
                "If the information is not present directly, it might be present in job description. Get it from there in that case\n"
                "For job role hash, you need to map the job role to the hash in the given data. You are given names of roles and "
                "their hashes in the variable existing_job_roles. Use this information to map the CLOSEST job role to the hash."
                "Mapping of the job role is compulsory\n"
                "Do not make up any data. If any data is missing, just fill it's value as None.\n"
                "For CTC, give the annual CTC response in INR. For example CTC of Rs. 5 Lakhs will be represented as 500000\n"
                "If CTC is in dollars, convert it to INR using the conversion 1 USD = 80 INR.\n"
                "CTC value won't be more than 100000000 in any case"
                "<Job Details>\n"
                f"Job Role: {role}\n"
                f"Existing Job Roles: {existing_job_roles}\n"
                f"Max CTC: {max_ctc}\n"
                f"Min CTC: {min_ctc}\n"
                f"CTC Currency: {ctc_currency}\n"
                f"Location: {location}\n"
                f"Skills: {skills}\n"
        )
        completion = self.openai_client.beta.chat.completions.parse(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                ],
                response_format=JobDetails
        )

        return completion.choices[0].message.parsed

    def _process_job(
            self, raw_job: RawJobOpening, company_slug: str
    ) -> ProcessedJobListing:
        raw_job_opening_data = raw_job.raw_data
        title = raw_job_opening_data.get("role")
        external_job_id = raw_job.external_job_id
        external_apply_link = raw_job_opening_data.get("careersPageLink")
        description = raw_job_opening_data.get("jobDetailsFromCompany")
        min_experience_years = raw_job_opening_data.get("minExp")
        max_experience_years = raw_job_opening_data.get("maxExp")
        job_details = self._extract_job_details_ai(raw_job)

        return ProcessedJobListing(
            external_job_id=external_job_id,
            title=title,
            company_slug=company_slug,
            role_hash=job_details.job_role_hash,
            description=description,
            min_ctc=job_details.min_ctc or 0,
            max_ctc=job_details.max_ctc or 0,
            external_apply_link=external_apply_link,
            city=job_details.city,
            state=job_details.state,
            employment_type=1,  # Currently only full-time jobs are supported
            min_experience_years=min_experience_years,
            max_experience_years=max_experience_years,
            skills=job_details.skills
        )
