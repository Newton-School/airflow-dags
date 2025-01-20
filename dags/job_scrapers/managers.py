from typing import Dict, List, Optional

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from openai import OpenAI
from pydantic import BaseModel

from .exceptions import CompanyCreateError
from .models import Company
from .utils import newton_api_request


class CompanyManager(LoggingMixin):
    """
    Manager class for companies.
    """

    def __init__(self, source: int, pg_conn_id: str = "postgres_job_posting"):
        super().__init__()
        self.source = source
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        self.openai_client = OpenAI(api_key=Variable.get("OPENAI_API_KEY"))
        self._company_cache: Dict[str, str] = {}
        self._load_companies()

    def _load_companies(self) -> None:
        """Load all companies from DB into memory"""
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                        """SELECT slug, normalized_names 
                            FROM airflow_companies
                        """
                )

                for slug, norm_names in cur.fetchall():
                    for norm_name in norm_names:
                        self._company_cache[norm_name] = slug

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize company name for matching."""
        # Convert to lowercase
        normalized = name.lower()

        # Remove common suffixes
        suffixes = [
                "private",
                "limited",
                "pvt",
                "ltd",
                "inc",
                "llc",
                "corporation",
                "corp",
        ]
        for suffix in suffixes:
            normalized = normalized.replace(f" {suffix} ", " ")
            normalized = normalized.replace(f" {suffix}.", " ")

        # Remove special characters but keep spaces
        normalized = "".join(c for c in normalized if c.isalnum() or c.isspace())

        return " ".join(normalized.split()).strip()

    def _get_ai_match(
            self, company_name: str, similar_companies: List[Dict[str, str]]
    ) -> Optional[str]:

        class CompanyMatchResponse(BaseModel):
            """
            Represents exact match company for the given company

            Fields:
            - slug: Optional[str]
            """

            slug: Optional[str]

        system_prompt = (
                "You are given a list of similar company names.\n"
                "You need to find the exact match for the given company name.\n"
                "For example Microsoft and Microsoft India are same companies\n"
                "Return the company slug for the exact match if it exists else return None.\n"
                "<Company Name>\n"
                f"{company_name}\n"
                "</Company Name>"
                "<Similar Companies>\n"
                f"{similar_companies}\n"
        )
        completion = self.openai_client.beta.chat.completions.parse(
                model="gpt-4o-mini",
                messages=[
                        {"role": "system", "content": system_prompt},
                ],
                response_format=CompanyMatchResponse
        )
        company_match_response = completion.choices[0].message.parsed
        return company_match_response.slug

    def _create_in_newton_api(self, company: Company) -> str:
        """
        Create company in Newton API.
        To be implemented with actual API call.
        """
        payload = {
                "title": company.name,
                "description": company.description,
                "status": 1,  # Actively hiring
                "source": self.source,
                "slug": company.slug,
                "logo_url": company.logo_url,
        }

        request_url = "/api/v1/job_scrapers/company/add/"
        response = newton_api_request(request_url, payload)

        if response.status_code not in [200, 201]:
            raise CompanyCreateError(f"Failed to create company: {response.text}")

        response_data = response.json()
        company_slug = response_data.get("slug")
        company.slug = company_slug

        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                        """INSERT INTO airflow_companies (slug, name, normalized_names, logo_url, website)
                            VALUES (%s, %s, %s, %s, %s)
                        """,
                        (company_slug, company.name, [self._normalize_name(company.name)], company.logo_url, company.website)
                )

        self._company_cache[self._normalize_name(company.name)] = company_slug
        return company_slug

    def get_similar_companies(self, company_name: str) -> List[Dict[str, str]]:
        """
        Get similar company names from DB.
        """
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                        """SELECT name, slug
                            FROM airflow_companies
                            WHERE similarity(name, %s) > 0.5
                            LIMIT 10
                        """,
                        (company_name,)
                )

                return [{"name": name, "slug": slug} for name, slug in cur.fetchall()]

    def get_or_create_company_slug(self, company: Company) -> str:
        """
        Get or create company slug based on name.
        First tries to match normalized name in cache.
        If not found, tries to match with similar names in DB.
        If not found, creates a new company in Newton API.
        """
        normalized = self._normalize_name(company.name)
        company_slug = self._company_cache.get(normalized)
        if company_slug:
            return company_slug

        similar_companies = self.get_similar_companies(company.name)
        if similar_companies:
            ai_match_company_slug = self._get_ai_match(company.name, similar_companies)
            if ai_match_company_slug:
                with self.pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                                """UPDATE airflow_companies
                                    SET normalized_names = normalized_names || %s
                                    WHERE slug = %s
                                """,
                                ([normalized], ai_match_company_slug)
                        )
                self._company_cache[normalized] = ai_match_company_slug
                return ai_match_company_slug

        return self._create_in_newton_api(company)
