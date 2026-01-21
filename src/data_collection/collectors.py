"""
Job data collectors from various sources.
We start with sample data, then can add real APIs later.
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator
from abc import ABC, abstractmethod

from .models import JobPosting


class BaseCollector(ABC):
    """
    Base class for all collectors.
    Think of this as a template that all collectors must follow.
    """
    
    @abstractmethod
    def collect(self) -> Generator[JobPosting, None, None]:
        """Collect job postings. Each collector implements this differently."""
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the data source."""
        pass


class SampleDataCollector(BaseCollector):
    """
    Generates realistic sample job data for development and testing.
    This lets us build the entire system without needing paid API access!
    """
    
    source_name = "sample_generator"
    
    # Realistic job data templates
    COMPANIES = [
        ("Google", "Mountain View, CA"), ("Meta", "Menlo Park, CA"),
        ("Amazon", "Seattle, WA"), ("Microsoft", "Redmond, WA"),
        ("Apple", "Cupertino, CA"), ("Netflix", "Los Gatos, CA"),
        ("Stripe", "San Francisco, CA"), ("Airbnb", "San Francisco, CA"),
        ("Uber", "San Francisco, CA"), ("Spotify", "New York, NY"),
        ("Adobe", "San Jose, CA"), ("Salesforce", "San Francisco, CA"),
        ("Twitter", "San Francisco, CA"), ("LinkedIn", "Sunnyvale, CA"),
        ("Snowflake", "San Mateo, CA"), ("Databricks", "San Francisco, CA"),
        ("OpenAI", "San Francisco, CA"), ("Anthropic", "San Francisco, CA"),
        ("Nvidia", "Santa Clara, CA"), ("Intel", "Santa Clara, CA"),
        ("IBM", "New York, NY"), ("Oracle", "Austin, TX"),
        ("Coinbase", "San Francisco, CA"), ("Robinhood", "Menlo Park, CA"),
        ("Plaid", "San Francisco, CA"), ("Figma", "San Francisco, CA"),
        ("Notion", "San Francisco, CA"), ("Slack", "San Francisco, CA"),
        ("Zoom", "San Jose, CA"), ("Shopify", "Remote"),
    ]
    
    JOB_TEMPLATES = {
        "Data Engineer": {
            "skills": ["python", "sql", "aws", "spark", "airflow", "docker", "kafka", "etl"],
            "salary_range": (120000, 200000),
            "description": """
            Join our data team to build scalable data pipelines!
            
            Responsibilities:
            - Design and implement ETL pipelines processing terabytes of data
            - Build real-time streaming systems using Kafka
            - Optimize data warehouse performance
            - Collaborate with ML engineers on feature pipelines
            
            Requirements:
            - {years}+ years of experience in data engineering
            - Strong Python and SQL skills
            - Experience with {cloud} cloud services
            - Knowledge of {tool1} and {tool2}
            - Understanding of data modeling and warehousing
            """
        },
        "Machine Learning Engineer": {
            "skills": ["python", "tensorflow", "pytorch", "mlops", "docker", "kubernetes", "aws"],
            "salary_range": (140000, 220000),
            "description": """
            Help us build the next generation of AI systems!
            
            Responsibilities:
            - Develop and deploy ML models at scale
            - Build MLOps pipelines for model training and serving
            - Optimize model performance and latency
            - Research and implement state-of-the-art techniques
            
            Requirements:
            - {years}+ years of ML engineering experience
            - Proficiency in Python and deep learning frameworks
            - Experience with {cloud} and containerization
            - Strong foundation in mathematics and statistics
            - Publications in top ML conferences is a plus
            """
        },
        "AI Engineer": {
            "skills": ["python", "langchain", "llm", "rag", "openai", "anthropic", "vector databases"],
            "salary_range": (150000, 250000),
            "description": """
            Build cutting-edge AI applications using LLMs!
            
            Responsibilities:
            - Design and implement RAG systems
            - Build AI agents and autonomous systems
            - Integrate LLMs into production applications
            - Optimize prompt engineering and model performance
            
            Requirements:
            - {years}+ years of experience with AI/ML
            - Deep knowledge of LLMs (GPT, Claude, etc.)
            - Experience with LangChain, vector databases
            - Understanding of embeddings and retrieval systems
            - Strong software engineering fundamentals
            """
        },
        "Data Scientist": {
            "skills": ["python", "sql", "statistics", "machine learning", "pandas", "scikit-learn"],
            "salary_range": (110000, 180000),
            "description": """
            Use data to drive business decisions!
            
            Responsibilities:
            - Analyze large datasets to find insights
            - Build predictive models for business problems
            - Design and analyze A/B tests
            - Create dashboards and reports for stakeholders
            
            Requirements:
            - {years}+ years in data science
            - Strong Python and SQL skills
            - Experience with statistical modeling
            - Excellent communication skills
            - Background in {domain} industry preferred
            """
        },
        "Backend Engineer": {
            "skills": ["python", "java", "api", "postgresql", "redis", "docker", "kubernetes"],
            "salary_range": (130000, 200000),
            "description": """
            Build robust backend systems that power our products!
            
            Responsibilities:
            - Design and implement scalable APIs
            - Optimize database performance
            - Build microservices architecture
            - Ensure system reliability and monitoring
            
            Requirements:
            - {years}+ years of backend development
            - Proficiency in {language} or similar
            - Experience with relational and NoSQL databases
            - Knowledge of cloud infrastructure
            - Strong system design skills
            """
        },
        "Full Stack Engineer": {
            "skills": ["javascript", "react", "node.js", "typescript", "postgresql", "aws"],
            "salary_range": (120000, 190000),
            "description": """
            Build end-to-end features that users love!
            
            Responsibilities:
            - Develop frontend and backend features
            - Create responsive and accessible UIs
            - Design and implement APIs
            - Write tests and documentation
            
            Requirements:
            - {years}+ years of full stack experience
            - Strong JavaScript/TypeScript skills
            - Experience with React and Node.js
            - Familiarity with cloud services
            - Eye for design and UX
            """
        },
        "DevOps Engineer": {
            "skills": ["aws", "terraform", "docker", "kubernetes", "ci/cd", "jenkins", "linux"],
            "salary_range": (125000, 195000),
            "description": """
            Build and maintain our cloud infrastructure!
            
            Responsibilities:
            - Design and manage cloud infrastructure
            - Implement CI/CD pipelines
            - Ensure system security and compliance
            - Automate operational tasks
            
            Requirements:
            - {years}+ years in DevOps/SRE
            - Strong experience with {cloud}
            - Proficiency with IaC tools (Terraform, etc.)
            - Container orchestration experience
            - Strong scripting skills
            """
        },
        "Analytics Engineer": {
            "skills": ["sql", "dbt", "python", "snowflake", "tableau", "data modeling"],
            "salary_range": (115000, 175000),
            "description": """
            Transform raw data into actionable insights!
            
            Responsibilities:
            - Build and maintain data models
            - Create dbt transformations
            - Design data warehouse schemas
            - Partner with stakeholders on metrics
            
            Requirements:
            - {years}+ years in analytics/BI
            - Expert SQL skills
            - Experience with dbt and modern data stack
            - Strong data modeling abilities
            - Excellent communication skills
            """
        }
    }
    
    EXPERIENCE_LEVELS = ["Entry", "Mid", "Senior", "Lead", "Principal"]
    CLOUDS = ["AWS", "GCP", "Azure"]
    TOOLS = ["Spark", "Airflow", "Kafka", "Flink", "dbt", "Snowflake", "Databricks"]
    LANGUAGES = ["Python", "Java", "Go", "Scala"]
    DOMAINS = ["fintech", "healthcare", "e-commerce", "adtech", "gaming"]
    
    def __init__(self, num_jobs: int = 500):
        """
        Initialize with number of jobs to generate.
        
        Args:
            num_jobs: How many fake job postings to create
        """
        self.num_jobs = num_jobs
    
    def collect(self) -> Generator[JobPosting, None, None]:
        """Generate sample job postings."""
        for _ in range(self.num_jobs):
            yield self._generate_job()
    
    def _generate_job(self) -> JobPosting:
        """Generate a single realistic job posting."""
        # Pick random job type
        title, template = random.choice(list(self.JOB_TEMPLATES.items()))
        company, base_location = random.choice(self.COMPANIES)
        
        # Determine experience level and adjust title
        exp_level = random.choice(self.EXPERIENCE_LEVELS)
        years = {"Entry": 1, "Mid": 3, "Senior": 5, "Lead": 7, "Principal": 10}[exp_level]
        
        if exp_level != "Mid":
            title = f"{exp_level} {title}"
        
        # Location variations
        is_remote = random.random() < 0.3  # 30% remote
        if is_remote:
            location = random.choice(["Remote", "Remote - US", f"Remote / {base_location}"])
        else:
            location = base_location
        
        # Generate salary (adjust by experience)
        base_min, base_max = template["salary_range"]
        exp_multiplier = {"Entry": 0.7, "Mid": 1.0, "Senior": 1.2, "Lead": 1.4, "Principal": 1.6}[exp_level]
        salary_min = int(base_min * exp_multiplier * random.uniform(0.9, 1.1))
        salary_max = int(base_max * exp_multiplier * random.uniform(0.9, 1.1))
        
        # Fill in description template
        description = template["description"].format(
            years=years,
            cloud=random.choice(self.CLOUDS),
            tool1=random.choice(self.TOOLS),
            tool2=random.choice(self.TOOLS),
            language=random.choice(self.LANGUAGES),
            domain=random.choice(self.DOMAINS)
        )
        
        # Random posted date (within last 30 days)
        days_ago = random.randint(0, 30)
        posted_date = datetime.now() - timedelta(days=days_ago)
        
        # Create job posting
        job = JobPosting(
            title=title,
            company=company,
            location=location,
            description=description,
            salary_min=salary_min,
            salary_max=salary_max,
            experience_level=exp_level,
            remote=is_remote,
            skills=template["skills"].copy(),
            source=self.source_name,
            posted_date=posted_date
        )
        
        # Extract additional skills from description
        job.extract_skills_from_description()
        
        return job


class DataCollectionManager:
    """
    Manages multiple collectors and coordinates data collection.
    Like a supervisor that tells all collectors to do their job.
    """
    
    def __init__(self):
        self.collectors: list[BaseCollector] = []
    
    def add_collector(self, collector: BaseCollector):
        """Add a new collector to the manager."""
        self.collectors.append(collector)
        print(f"✅ Added collector: {collector.source_name}")
    
    def collect_all(self) -> list[JobPosting]:
        """Run all collectors and gather all job postings."""
        all_jobs = []
        
        for collector in self.collectors:
            print(f"📥 Collecting from {collector.source_name}...")
            jobs = list(collector.collect())
            all_jobs.extend(jobs)
            print(f"   Found {len(jobs)} jobs")
        
        print(f"\n✨ Total jobs collected: {len(all_jobs)}")
        return all_jobs
    
    def save_to_json(self, jobs: list[JobPosting], filepath: str):
        """Save collected jobs to a JSON file."""
        data = [job.to_dict() for job in jobs]
        
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"💾 Saved {len(jobs)} jobs to {filepath}")


# Test the collector
if __name__ == "__main__":
    print("🚀 Testing Data Collection System\n")
    
    # Create manager and add sample collector
    manager = DataCollectionManager()
    manager.add_collector(SampleDataCollector(num_jobs=100))
    
    # Collect jobs
    jobs = manager.collect_all()
    
    # Save to file
    manager.save_to_json(jobs, "data/raw/sample_jobs.json")
    
    # Show some examples
    print("\n📋 Sample Jobs:")
    for job in jobs[:3]:
        print(f"\n{'='*50}")
        print(f"📌 {job.title} at {job.company}")
        print(f"📍 {job.location} | 💰 ${job.salary_min:,} - ${job.salary_max:,}")
        print(f"🔧 Skills: {', '.join(job.skills[:5])}")