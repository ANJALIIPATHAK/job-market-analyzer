"""
Real Job Data Collectors - Fetches actual job postings from FREE sources.

Sources used:
1. Remotive API (FREE) - Remote tech jobs
2. Arbeitnow API (FREE) - Tech jobs in Europe/US
3. GitHub Jobs (via alternative) - Developer jobs
"""
import requests
import time
from datetime import datetime
from typing import Generator
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.data_collection.models import JobPosting
from src.data_collection.collectors import BaseCollector


class RemotiveCollector(BaseCollector):
    """
    Collects remote tech jobs from Remotive.com API.
    100% FREE, no API key required!
    """
    
    source_name = "remotive"
    BASE_URL = "https://remotive.com/api/remote-jobs"
    
    # Job categories available on Remotive
    CATEGORIES = [
        "software-dev",
        "data",
        "devops",
        "product",
        "marketing",
        "customer-support",
        "design",
        "qa"
    ]
    
    def __init__(self, categories: list = None, limit_per_category: int = 50):
        """
        Initialize collector.
        
        Args:
            categories: List of job categories to fetch (default: software-dev, data, devops)
            limit_per_category: Max jobs per category
        """
        self.categories = categories or ["software-dev", "data", "devops"]
        self.limit = limit_per_category
    
    def collect(self) -> Generator[JobPosting, None, None]:
        """Fetch real remote jobs from Remotive API."""
        
        for category in self.categories:
            print(f"  📥 Fetching {category} jobs from Remotive...")
            
            try:
                response = requests.get(
                    self.BASE_URL,
                    params={"category": category, "limit": self.limit},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                jobs = data.get("jobs", [])
                print(f"     Found {len(jobs)} jobs")
                
                for job_data in jobs:
                    job = self._parse_job(job_data)
                    if job:
                        yield job
                
                # Be nice to the API
                time.sleep(1)
                
            except requests.RequestException as e:
                print(f"     ❌ Error fetching {category}: {e}")
                continue
    
    def _parse_job(self, data: dict) -> JobPosting:
        """Convert API response to JobPosting."""
        try:
            # Parse salary if available
            salary_text = data.get("salary", "")
            salary_min, salary_max = self._parse_salary(salary_text)
            
            # Determine experience level from title
            title = data.get("title", "")
            exp_level = self._guess_experience_level(title)
            
            # Parse date
            posted_date = None
            if data.get("publication_date"):
                try:
                    posted_date = datetime.fromisoformat(
                        data["publication_date"].replace("Z", "+00:00")
                    )
                except:
                    pass
            
            job = JobPosting(
                title=title,
                company=data.get("company_name", "Unknown"),
                location=data.get("candidate_required_location", "Remote"),
                description=data.get("description", ""),
                salary_min=salary_min,
                salary_max=salary_max,
                experience_level=exp_level,
                remote=True,  # All Remotive jobs are remote
                job_type=data.get("job_type", "full_time").replace("_", "-").title(),
                source=self.source_name,
                url=data.get("url", ""),
                posted_date=posted_date
            )
            
            # Extract skills from description
            job.extract_skills_from_description()
            
            return job
            
        except Exception as e:
            print(f"     ⚠️ Error parsing job: {e}")
            return None
    
    def _parse_salary(self, salary_text: str) -> tuple:
        """Extract salary range from text."""
        if not salary_text:
            return None, None
        
        import re
        # Look for patterns like "$100,000 - $150,000" or "100k-150k"
        numbers = re.findall(r'[\d,]+(?:k)?', salary_text.lower())
        
        parsed = []
        for num in numbers:
            num = num.replace(",", "")
            if num.endswith('k'):
                parsed.append(float(num[:-1]) * 1000)
            else:
                try:
                    val = float(num)
                    # If it's a small number, assume it's in thousands
                    if val < 1000:
                        val *= 1000
                    parsed.append(val)
                except:
                    continue
        
        if len(parsed) >= 2:
            return min(parsed), max(parsed)
        elif len(parsed) == 1:
            return parsed[0], parsed[0] * 1.2  # Estimate range
        
        return None, None
    
    def _guess_experience_level(self, title: str) -> str:
        """Guess experience level from job title."""
        title_lower = title.lower()
        
        if any(x in title_lower for x in ['senior', 'sr.', 'sr ', 'lead', 'principal', 'staff']):
            if 'principal' in title_lower or 'staff' in title_lower:
                return 'Principal'
            elif 'lead' in title_lower:
                return 'Lead'
            return 'Senior'
        elif any(x in title_lower for x in ['junior', 'jr.', 'jr ', 'entry', 'associate', 'intern']):
            return 'Entry'
        else:
            return 'Mid'


class ArbeitnowCollector(BaseCollector):
    """
    Collects tech jobs from Arbeitnow API.
    FREE API with tech jobs from Europe and US.
    """
    
    source_name = "arbeitnow"
    BASE_URL = "https://www.arbeitnow.com/api/job-board-api"
    
    def __init__(self, max_pages: int = 5):
        """
        Initialize collector.
        
        Args:
            max_pages: Maximum pages to fetch (each page ~100 jobs)
        """
        self.max_pages = max_pages
    
    def collect(self) -> Generator[JobPosting, None, None]:
        """Fetch jobs from Arbeitnow API."""
        
        for page in range(1, self.max_pages + 1):
            print(f"  📥 Fetching page {page} from Arbeitnow...")
            
            try:
                response = requests.get(
                    self.BASE_URL,
                    params={"page": page},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                jobs = data.get("data", [])
                print(f"     Found {len(jobs)} jobs")
                
                if not jobs:
                    break
                
                for job_data in jobs:
                    # Filter for tech jobs only
                    if self._is_tech_job(job_data):
                        job = self._parse_job(job_data)
                        if job:
                            yield job
                
                time.sleep(1)
                
            except requests.RequestException as e:
                print(f"     ❌ Error fetching page {page}: {e}")
                continue
    
    def _is_tech_job(self, data: dict) -> bool:
        """Check if job is tech-related."""
        tech_keywords = [
            'developer', 'engineer', 'programmer', 'software', 'data',
            'devops', 'cloud', 'python', 'java', 'javascript', 'backend',
            'frontend', 'fullstack', 'machine learning', 'ai ', 'ml ',
            'analyst', 'architect', 'security', 'database', 'api'
        ]
        
        title = data.get("title", "").lower()
        tags = " ".join(data.get("tags", [])).lower()
        
        return any(kw in title or kw in tags for kw in tech_keywords)
    
    def _parse_job(self, data: dict) -> JobPosting:
        """Convert API response to JobPosting."""
        try:
            title = data.get("title", "")
            
            job = JobPosting(
                title=title,
                company=data.get("company_name", "Unknown"),
                location=data.get("location", "Unknown"),
                description=data.get("description", ""),
                remote=data.get("remote", False),
                experience_level=self._guess_experience_level(title),
                source=self.source_name,
                url=data.get("url", ""),
                posted_date=datetime.now()  # API doesn't provide exact date
            )
            
            # Add tags as skills
            tags = data.get("tags", [])
            job.skills = [t.lower() for t in tags]
            
            # Also extract from description
            job.extract_skills_from_description()
            
            return job
            
        except Exception as e:
            print(f"     ⚠️ Error parsing job: {e}")
            return None
    
    def _guess_experience_level(self, title: str) -> str:
        """Guess experience level from job title."""
        title_lower = title.lower()
        
        if any(x in title_lower for x in ['senior', 'sr.', 'lead', 'principal', 'staff']):
            if 'principal' in title_lower or 'staff' in title_lower:
                return 'Principal'
            elif 'lead' in title_lower:
                return 'Lead'
            return 'Senior'
        elif any(x in title_lower for x in ['junior', 'jr.', 'entry', 'intern']):
            return 'Entry'
        else:
            return 'Mid'


def fetch_real_jobs(include_remotive: bool = True, include_arbeitnow: bool = True) -> list[JobPosting]:
    """
    Convenience function to fetch jobs from all real sources.
    
    Args:
        include_remotive: Include Remotive jobs
        include_arbeitnow: Include Arbeitnow jobs
    
    Returns:
        List of real job postings
    """
    from src.data_collection.collectors import DataCollectionManager
    
    manager = DataCollectionManager()
    
    if include_remotive:
        manager.add_collector(RemotiveCollector(
            categories=["software-dev", "data", "devops"],
            limit_per_category=100
        ))
    
    if include_arbeitnow:
        manager.add_collector(ArbeitnowCollector(max_pages=3))
    
    return manager.collect_all()


# Test the real collectors
if __name__ == "__main__":
    from src.etl.database import JobDatabase
    from src.rag.vector_store import JobVectorStore
    
    print("🚀 Fetching REAL Job Data\n")
    print("="*60)
    
    # Fetch real jobs
    jobs = fetch_real_jobs()
    
    print(f"\n✅ Total real jobs collected: {len(jobs)}")
    
    if jobs:
        # Show some examples
        print("\n📋 Sample Real Jobs:")
        for job in jobs[:5]:
            print(f"\n{'='*50}")
            print(f"📌 {job.title}")
            print(f"🏢 {job.company}")
            print(f"📍 {job.location}")
            print(f"🔧 Skills: {', '.join(job.skills[:5])}")
            if job.salary_min and job.salary_max:
                print(f"💰 ${job.salary_min:,.0f} - ${job.salary_max:,.0f}")
        
        # Ask if user wants to save to database
        print("\n" + "="*60)
        save = input("💾 Save these jobs to database? (y/n): ").strip().lower()
        
        if save == 'y':
            print("\n📥 Saving to database...")
            db = JobDatabase()
            inserted, skipped = db.insert_many(jobs)
            
            print("\n📥 Adding to vector store...")
            vs = JobVectorStore()
            vs.add_jobs(jobs)
            
            print(f"\n✅ Done! Added {inserted} new jobs to database and vector store.")
        else:
            print("👍 Okay, jobs not saved.")
    else:
        print("❌ No jobs were collected. Check your internet connection.")