"""
Database layer for storing and retrieving job postings.
Uses SQLite - a simple, file-based database (no server needed!).
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.data_collection.models import JobPosting


class JobDatabase:
    """
    Handles all database operations for job postings.
    Think of it as a filing cabinet for our jobs.
    """
    
    def __init__(self, db_path: str = "data/jobs.db"):
        """
        Initialize database connection.
        
        Args:
            db_path: Where to store the database file
        """
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """Create a database connection (safely closes when done)."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """Create tables if they don't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Main jobs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    company TEXT NOT NULL,
                    location TEXT,
                    description TEXT,
                    salary_min REAL,
                    salary_max REAL,
                    salary_currency TEXT DEFAULT 'USD',
                    job_type TEXT DEFAULT 'Full-time',
                    experience_level TEXT,
                    remote INTEGER DEFAULT 0,
                    skills TEXT,
                    source TEXT,
                    url TEXT,
                    posted_date TEXT,
                    scraped_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Skills table (for analytics)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_skills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    skill TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs(id)
                )
            ''')
            
            # Create indexes for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(location)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_experience ON jobs(experience_level)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_skills_skill ON job_skills(skill)')
            
            conn.commit()
            print("✅ Database initialized")
    
    def insert_job(self, job: JobPosting) -> bool:
        """
        Insert a single job into the database.
        Returns True if inserted, False if already exists.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Insert job
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs 
                    (id, title, company, location, description, salary_min, salary_max,
                     salary_currency, job_type, experience_level, remote, skills,
                     source, url, posted_date, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job.id, job.title, job.company, job.location, job.description,
                    job.salary_min, job.salary_max, job.salary_currency, job.job_type,
                    job.experience_level, int(job.remote), json.dumps(job.skills),
                    job.source, job.url,
                    job.posted_date.isoformat() if job.posted_date else None,
                    job.scraped_at.isoformat()
                ))
                
                # Insert skills (for easier querying)
                if cursor.rowcount > 0:  # Only if job was actually inserted
                    for skill in job.skills:
                        cursor.execute('''
                            INSERT INTO job_skills (job_id, skill)
                            VALUES (?, ?)
                        ''', (job.id, skill.lower()))
                
                conn.commit()
                return cursor.rowcount > 0
                
            except sqlite3.Error as e:
                print(f"❌ Error inserting job: {e}")
                return False
    
    def insert_many(self, jobs: list[JobPosting]) -> tuple[int, int]:
        """
        Insert multiple jobs efficiently.
        Returns (inserted_count, skipped_count).
        """
        inserted = 0
        skipped = 0
        
        for job in jobs:
            if self.insert_job(job):
                inserted += 1
            else:
                skipped += 1
        
        print(f"📊 Inserted: {inserted}, Skipped (duplicates): {skipped}")
        return inserted, skipped
    
    def get_job(self, job_id: str) -> Optional[JobPosting]:
        """Get a single job by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM jobs WHERE id = ?', (job_id,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_job(row)
            return None
    
    def search_jobs(
        self,
        query: Optional[str] = None,
        company: Optional[str] = None,
        location: Optional[str] = None,
        skills: Optional[list[str]] = None,
        experience_level: Optional[str] = None,
        remote_only: bool = False,
        min_salary: Optional[float] = None,
        limit: int = 100
    ) -> list[JobPosting]:
        """
        Search jobs with various filters.
        This is how our AI agent will query the database!
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Build dynamic query
            conditions = []
            params = []
            
            if query:
                conditions.append("(title LIKE ? OR description LIKE ? OR company LIKE ?)")
                search = f"%{query}%"
                params.extend([search, search, search])
            
            if company:
                conditions.append("company LIKE ?")
                params.append(f"%{company}%")
            
            if location:
                conditions.append("location LIKE ?")
                params.append(f"%{location}%")
            
            if experience_level:
                conditions.append("experience_level = ?")
                params.append(experience_level)
            
            if remote_only:
                conditions.append("remote = 1")
            
            if min_salary:
                conditions.append("salary_min >= ?")
                params.append(min_salary)
            
            # Build final query
            sql = "SELECT * FROM jobs"
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += f" ORDER BY scraped_at DESC LIMIT {limit}"
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Filter by skills if specified (need special handling)
            jobs = [self._row_to_job(row) for row in rows]
            
            if skills:
                skills_lower = [s.lower() for s in skills]
                jobs = [
                    job for job in jobs
                    if any(s in [sk.lower() for sk in job.skills] for s in skills_lower)
                ]
            
            return jobs
    
    def get_all_jobs(self, limit: int = 1000) -> list[JobPosting]:
        """Get all jobs (up to limit)."""
        return self.search_jobs(limit=limit)
    
    def get_stats(self) -> dict:
        """Get database statistics - useful for analytics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total jobs
            cursor.execute("SELECT COUNT(*) FROM jobs")
            stats['total_jobs'] = cursor.fetchone()[0]
            
            # Jobs by experience level
            cursor.execute("""
                SELECT experience_level, COUNT(*) as count 
                FROM jobs 
                GROUP BY experience_level
                ORDER BY count DESC
            """)
            stats['by_experience'] = dict(cursor.fetchall())
            
            # Top companies
            cursor.execute("""
                SELECT company, COUNT(*) as count 
                FROM jobs 
                GROUP BY company 
                ORDER BY count DESC 
                LIMIT 10
            """)
            stats['top_companies'] = dict(cursor.fetchall())
            
            # Top skills
            cursor.execute("""
                SELECT skill, COUNT(*) as count 
                FROM job_skills 
                GROUP BY skill 
                ORDER BY count DESC 
                LIMIT 20
            """)
            stats['top_skills'] = dict(cursor.fetchall())
            
            # Remote vs On-site
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN remote = 1 THEN 1 ELSE 0 END) as remote,
                    SUM(CASE WHEN remote = 0 THEN 1 ELSE 0 END) as onsite
                FROM jobs
            """)
            row = cursor.fetchone()
            stats['remote_distribution'] = {'remote': row[0], 'on_site': row[1]}
            
            # Average salary by experience
            cursor.execute("""
                SELECT experience_level, 
                       AVG(salary_min) as avg_min, 
                       AVG(salary_max) as avg_max
                FROM jobs 
                WHERE salary_min IS NOT NULL
                GROUP BY experience_level
            """)
            stats['salary_by_experience'] = {
                row[0]: {'min': round(row[1], 0), 'max': round(row[2], 0)}
                for row in cursor.fetchall()
            }
            
            return stats
    
    def _row_to_job(self, row: sqlite3.Row) -> JobPosting:
        """Convert database row to JobPosting object."""
        data = dict(row)
        data['remote'] = bool(data['remote'])
        data['skills'] = json.loads(data['skills']) if data['skills'] else []
        
        # Handle dates
        if data.get('posted_date'):
            data['posted_date'] = datetime.fromisoformat(data['posted_date'])
        if data.get('scraped_at'):
            data['scraped_at'] = datetime.fromisoformat(data['scraped_at'])
        
        # Remove fields not in JobPosting
        data.pop('created_at', None)
        data.pop('id', None)  # Will be regenerated
        
        return JobPosting(**data)
    
    def clear_all(self):
        """Delete all data (use carefully!)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM job_skills")
            cursor.execute("DELETE FROM jobs")
            conn.commit()
            print("🗑️ All data cleared")


# Test the database
if __name__ == "__main__":
    from src.data_collection.collectors import SampleDataCollector, DataCollectionManager
    
    print("🚀 Testing Database System\n")
    
    # Create database
    db = JobDatabase("data/jobs.db")
    
    # Generate and insert sample jobs
    collector = SampleDataCollector(num_jobs=200)
    jobs = list(collector.collect())
    
    print(f"\n📥 Inserting {len(jobs)} jobs into database...")
    db.insert_many(jobs)
    
    # Test search
    print("\n🔍 Testing search functionality:")
    
    # Search by skill
    python_jobs = db.search_jobs(skills=["python"], limit=5)
    print(f"\nPython jobs: {len(python_jobs)}")
    for job in python_jobs[:2]:
        print(f"  - {job.title} at {job.company}")
    
    # Search remote jobs
    remote_jobs = db.search_jobs(remote_only=True, limit=5)
    print(f"\nRemote jobs: {len(remote_jobs)}")
    
    # Search senior positions
    senior_jobs = db.search_jobs(experience_level="Senior", limit=5)
    print(f"\nSenior positions: {len(senior_jobs)}")
    
    # Get stats
    print("\n📊 Database Statistics:")
    stats = db.get_stats()
    print(f"Total jobs: {stats['total_jobs']}")
    print(f"Top skills: {list(stats['top_skills'].keys())[:5]}")
    print(f"Top companies: {list(stats['top_companies'].keys())[:5]}")