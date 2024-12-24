from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Optional


@dataclass
class RawJobOpening:
    """
    Represents a raw job opening exactly as scraped from the source.
    """

    external_job_id: str
    source_name: str
    raw_data: Dict[str, Any]
    is_external_for_job_board: bool

    def __post_init__(self):
        """Validate essential fields"""
        if not self.external_job_id:
            raise ValueError("external_job_id cannot be empty")
        if not self.source_name:
            raise ValueError("source_name cannot be empty")
        if self.is_external_for_job_board is None:
            raise ValueError("is_external_for_job_board cannot be empty")
        if not isinstance(self.raw_data, dict):
            raise ValueError("raw_data must be a dictionary")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        return {
            "external_job_id": self.external_job_id,
            "source_name": self.source_name,
            "raw_data": self.raw_data,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RawJobOpening":
        """Create instance from dictionary"""
        return cls(**data)


class EmploymentType(Enum):
    """Enum for employment types"""

    FULL_TIME = 1
    INTERNSHIP = 2


@dataclass
class ProcessedJobListing:
    """
    Represents a job listing after processing/transformation.
    Contains standardized fields ready for Newton API.
    """

    external_job_id: str
    title: str
    company_slug: str
    role_hash: str
    description: str
    min_ctc: Optional[int]
    max_ctc: Optional[int]
    external_apply_link: str
    city: Optional[str]
    state: Optional[str]
    employment_type: int = EmploymentType.FULL_TIME.value
    min_experience_years: Optional[int] = None
    max_experience_years: Optional[int] = None
    skills: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate essential fields"""
        if not self.title:
            raise ValueError("title cannot be empty")
        if not self.company_slug:
            raise ValueError("company_slug cannot be empty")
        if not self.role_hash:
            raise ValueError("role_hash cannot be empty")
        if not self.description:
            raise ValueError("description cannot be empty")
        self.skills = self.skills or []


@dataclass
class Company:
    """
    Represents a company in the system.
    """

    slug: str
    name: str
    normalized_names: List[str] = field(default_factory=list)
    website: Optional[str] = None
    description: Optional[str] = None
    logo_url: Optional[str] = None

    def __post_init__(self):
        """Validate essential fields"""
        if not self.slug:
            raise ValueError("slug cannot be empty")
        if not self.name:
            raise ValueError("name cannot be empty")
