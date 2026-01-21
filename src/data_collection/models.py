"""
Data models for job postings.
Think of these as blueprints that define what a "job posting" looks like.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
import hashlib
import json


@dataclass
class JobPosting:
    """
    Represents a single job posting.
    
    Like a form with fields to fill out for each job we find.
    """
    title: str
    company: str
    location: str
    description: str
    
    # Optional fields (might not always have these)
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: str = "USD"
    job_type: str = "Full-time"  # Full-time, Part-time, Contract
    experience_level: str = "Mid"  # Entry, Mid, Senior, Lead
    remote: bool = False
    skills: list = field(default_factory=list)
    
    # Metadata
    source: str = "unknown"  # Where we found this job
    url: Optional[str] = None
    posted_date: Optional[datetime] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    
    # Unique identifier (generated automatically)
    id: str = field(default="", init=False)
    
    def __post_init__(self):
        """Generate unique ID after creating the job posting."""
        import uuid
        # Create a unique ID using UUID + content hash for guaranteed uniqueness
        content = f"{self.title}{self.company}{self.location}{uuid.uuid4()}"
        self.id = hashlib.md5(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> dict:
        """Convert to dictionary (useful for saving to database)."""
        data = asdict(self)
        # Convert datetime to string for JSON compatibility
        if self.posted_date:
            data['posted_date'] = self.posted_date.isoformat()
        data['scraped_at'] = self.scraped_at.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'JobPosting':
        """Create JobPosting from dictionary."""
        # Handle datetime conversion
        if isinstance(data.get('posted_date'), str):
            data['posted_date'] = datetime.fromisoformat(data['posted_date'])
        if isinstance(data.get('scraped_at'), str):
            data['scraped_at'] = datetime.fromisoformat(data['scraped_at'])
        
        # Remove 'id' as it's auto-generated
        data.pop('id', None)
        return cls(**data)
    
    def extract_skills_from_description(self) -> list:
        """
        Extract common tech skills from job description.
        This is a simple version - we'll make it smarter later!
        """
        common_skills = [
            'python', 'javascript', 'java', 'c++', 'sql', 'aws', 'azure',
            'docker', 'kubernetes', 'react', 'node.js', 'typescript',
            'machine learning', 'deep learning', 'nlp', 'computer vision',
            'tensorflow', 'pytorch', 'pandas', 'numpy', 'scikit-learn',
            'git', 'linux', 'agile', 'scrum', 'api', 'rest', 'graphql',
            'postgresql', 'mongodb', 'redis', 'elasticsearch', 'spark',
            'airflow', 'kafka', 'ci/cd', 'jenkins', 'terraform',
            'langchain', 'llm', 'rag', 'openai', 'anthropic', 'gpt',
            'data engineering', 'data science', 'analytics', 'etl',
            'power bi', 'tableau', 'excel', 'statistics', 'a/b testing'
        ]
        
        desc_lower = self.description.lower()
        found = [s for s in common_skills if s in desc_lower]
        self.skills = list(set(found))  # Remove duplicates
        return self.skills


# Example usage (this runs only if you run this file directly)
if __name__ == "__main__":
    # Create a sample job
    sample_job = JobPosting(
        title="Senior Data Engineer",
        company="TechCorp",
        location="San Francisco, CA",
        description="""
        We're looking for a Senior Data Engineer to join our team!
        
        Requirements:
        - 5+ years experience with Python and SQL
        - Experience with AWS, Spark, and Airflow
        - Knowledge of Docker and Kubernetes
        - Familiarity with machine learning pipelines
        """,
        salary_min=150000,
        salary_max=200000,
        remote=True,
        experience_level="Senior"
    )
    
    # Extract skills automatically
    sample_job.extract_skills_from_description()
    
    # Print the job
    print("=== Sample Job Posting ===")
    print(sample_job.to_json())
    print(f"\n🔧 Skills found: {sample_job.skills}")