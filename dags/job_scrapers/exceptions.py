class JobScrapingError(Exception):
    """Base exception for job scraping errors"""

    pass


class RateLimitError(JobScrapingError):
    """Raised when rate limits are hit"""

    pass


class ConfigError(JobScrapingError):
    """Raised for configuration issues"""

    pass


# Base exception for all job-processing related operations
class JobProcessingError(Exception):
    """Base class for all job processing related errors"""

    pass


class TransformationError(JobProcessingError):
    """Base class for all transformation related errors"""

    pass


class CompanyMatchError(TransformationError):
    """Raised when there's an error in company matching/creation process"""

    pass


class ValidationError(TransformationError):
    """Raised when job data fails validation"""

    pass


class CompanyCreateError(TransformationError):
    """Raised when company creation fails"""

    pass
