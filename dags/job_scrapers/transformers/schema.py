# Schema for openai job transformer
from typing import Optional, List

from pydantic import BaseModel


class JobDetails(BaseModel):
    """
    Represents details of a job opening.

    Fields:
    - job_role_hash: (str) Hash of the job role. This cannot be empty
    - max_ctc: (Optional[int]) Maximum CTC offered by the company in INR. For example, Rs. 5 Lakhs will be represented as 500000
    - min_ctc: (Optional[int]) Minimum CTC offered by the company in INR. For example, Rs. 5 Lakhs will be represented as 500000
    - city: (Optional[str]) City where the job is located.
    - state: (Optional[str]) State where the job is located.
    - skills: List[str]: List of skills required for the job.
    - job_description: (str) Description of the job
    """

    job_role_hash: str
    max_ctc: Optional[int]
    min_ctc: Optional[int]
    city: Optional[str]
    state: Optional[str]
    skills: List[str]
    job_description: str
