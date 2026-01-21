"""
Analytics Engine - Generates insights and statistics from job data.
Used by both the AI agent and the web interface.
"""
import pandas as pd
from pathlib import Path
from typing import Optional
from collections import Counter

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.etl.database import JobDatabase


class JobAnalyzer:
    """
    Analyzes job market data and generates insights.
    """
    
    def __init__(self, database: Optional[JobDatabase] = None):
        self.db = database or JobDatabase()
        print("✅ Job Analyzer initialized")
    
    def get_jobs_dataframe(self) -> pd.DataFrame:
        """Get all jobs as a pandas DataFrame for analysis."""
        jobs = self.db.get_all_jobs(limit=10000)
        
        data = []
        for job in jobs:
            data.append({
                'id': job.id,
                'title': job.title,
                'company': job.company,
                'location': job.location,
                'salary_min': job.salary_min,
                'salary_max': job.salary_max,
                'experience_level': job.experience_level,
                'remote': job.remote,
                'skills': job.skills,
                'posted_date': job.posted_date,
                'source': job.source
            })
        
        return pd.DataFrame(data)
    
    def get_skill_demand(self, top_n: int = 20) -> dict:
        """Get the most in-demand skills."""
        stats = self.db.get_stats()
        top_skills = dict(list(stats.get('top_skills', {}).items())[:top_n])
        return top_skills
    
    def get_salary_by_experience(self) -> dict:
        """Get average salaries by experience level."""
        stats = self.db.get_stats()
        return stats.get('salary_by_experience', {})
    
    def get_salary_by_role(self) -> dict:
        """Get average salaries by job role/title."""
        df = self.get_jobs_dataframe()
        
        # Extract base role from title
        def get_base_role(title):
            title_lower = title.lower()
            if 'data engineer' in title_lower:
                return 'Data Engineer'
            elif 'machine learning' in title_lower or 'ml engineer' in title_lower:
                return 'ML Engineer'
            elif 'ai engineer' in title_lower:
                return 'AI Engineer'
            elif 'data scientist' in title_lower:
                return 'Data Scientist'
            elif 'backend' in title_lower:
                return 'Backend Engineer'
            elif 'full stack' in title_lower or 'fullstack' in title_lower:
                return 'Full Stack Engineer'
            elif 'devops' in title_lower or 'sre' in title_lower:
                return 'DevOps Engineer'
            elif 'analytics' in title_lower:
                return 'Analytics Engineer'
            else:
                return 'Other'
        
        df['role'] = df['title'].apply(get_base_role)
        
        salary_by_role = df.groupby('role').agg({
            'salary_min': 'mean',
            'salary_max': 'mean'
        }).round(0).to_dict('index')
        
        return salary_by_role
    
    def get_top_companies(self, top_n: int = 10) -> dict:
        """Get companies with most job openings."""
        stats = self.db.get_stats()
        return dict(list(stats.get('top_companies', {}).items())[:top_n])
    
    def get_location_distribution(self) -> dict:
        """Get job distribution by location."""
        df = self.get_jobs_dataframe()
        
        # Simplify locations
        def simplify_location(loc):
            if not loc:
                return 'Unknown'
            loc_lower = loc.lower()
            if 'remote' in loc_lower:
                return 'Remote'
            elif 'san francisco' in loc_lower or 'sf' in loc_lower:
                return 'San Francisco'
            elif 'new york' in loc_lower or 'nyc' in loc_lower:
                return 'New York'
            elif 'seattle' in loc_lower:
                return 'Seattle'
            elif 'austin' in loc_lower:
                return 'Austin'
            elif 'los angeles' in loc_lower or 'la' in loc_lower:
                return 'Los Angeles'
            else:
                return loc.split(',')[0]  # Just city name
        
        df['simple_location'] = df['location'].apply(simplify_location)
        location_counts = df['simple_location'].value_counts().head(10).to_dict()
        
        return location_counts
    
    def get_remote_stats(self) -> dict:
        """Get remote vs on-site statistics."""
        stats = self.db.get_stats()
        remote_dist = stats.get('remote_distribution', {})
        
        remote = remote_dist.get('remote', 0)
        onsite = remote_dist.get('on_site', 0)
        total = remote + onsite
        
        return {
            'remote_count': remote,
            'onsite_count': onsite,
            'remote_percentage': round(remote / total * 100, 1) if total > 0 else 0,
            'onsite_percentage': round(onsite / total * 100, 1) if total > 0 else 0
        }
    
    def get_experience_distribution(self) -> dict:
        """Get job distribution by experience level."""
        stats = self.db.get_stats()
        return stats.get('by_experience', {})
    
    def get_skill_salary_correlation(self) -> dict:
        """Analyze which skills correlate with higher salaries."""
        df = self.get_jobs_dataframe()
        
        # Calculate average salary for each skill
        skill_salaries = {}
        
        for _, row in df.iterrows():
            if row['salary_max'] and row['skills']:
                avg_salary = (row['salary_min'] + row['salary_max']) / 2
                for skill in row['skills']:
                    if skill not in skill_salaries:
                        skill_salaries[skill] = []
                    skill_salaries[skill].append(avg_salary)
        
        # Calculate averages
        skill_avg_salary = {
            skill: round(sum(salaries) / len(salaries), 0)
            for skill, salaries in skill_salaries.items()
            if len(salaries) >= 5  # Only skills with enough data
        }
        
        # Sort by salary
        sorted_skills = dict(sorted(
            skill_avg_salary.items(),
            key=lambda x: x[1],
            reverse=True
        )[:15])
        
        return sorted_skills
    
    def get_trending_skills(self) -> dict:
        """
        Identify trending skills based on recent job postings.
        In a real scenario, this would compare recent vs older data.
        """
        # For now, return skills weighted by recency
        df = self.get_jobs_dataframe()
        df = df.sort_values('posted_date', ascending=False)
        
        # Get skills from most recent 50% of jobs
        recent_df = df.head(len(df) // 2)
        
        recent_skills = []
        for skills in recent_df['skills']:
            if skills:
                recent_skills.extend(skills)
        
        skill_counts = Counter(recent_skills)
        return dict(skill_counts.most_common(10))
    
    def generate_market_report(self) -> dict:
        """Generate a comprehensive market report."""
        return {
            'total_jobs': self.db.get_stats().get('total_jobs', 0),
            'top_skills': self.get_skill_demand(10),
            'salary_by_experience': self.get_salary_by_experience(),
            'salary_by_role': self.get_salary_by_role(),
            'top_companies': self.get_top_companies(10),
            'location_distribution': self.get_location_distribution(),
            'remote_stats': self.get_remote_stats(),
            'experience_distribution': self.get_experience_distribution(),
            'highest_paying_skills': self.get_skill_salary_correlation(),
            'trending_skills': self.get_trending_skills()
        }
    
    def get_role_comparison(self, role1: str, role2: str) -> dict:
        """Compare two roles side by side."""
        df = self.get_jobs_dataframe()
        
        def filter_role(title, role):
            return role.lower() in title.lower()
        
        df1 = df[df['title'].apply(lambda x: filter_role(x, role1))]
        df2 = df[df['title'].apply(lambda x: filter_role(x, role2))]
        
        def get_role_stats(role_df, role_name):
            if len(role_df) == 0:
                return {'role': role_name, 'count': 0}
            
            # Collect all skills
            all_skills = []
            for skills in role_df['skills']:
                if skills:
                    all_skills.extend(skills)
            
            return {
                'role': role_name,
                'count': len(role_df),
                'avg_salary_min': round(role_df['salary_min'].mean(), 0),
                'avg_salary_max': round(role_df['salary_max'].mean(), 0),
                'remote_percentage': round(role_df['remote'].mean() * 100, 1),
                'top_skills': dict(Counter(all_skills).most_common(5))
            }
        
        return {
            'role1': get_role_stats(df1, role1),
            'role2': get_role_stats(df2, role2)
        }


# Test the analyzer
if __name__ == "__main__":
    print("🚀 Testing Analytics Engine\n")
    print("="*60)
    
    analyzer = JobAnalyzer()
    
    # Generate full report
    print("\n📊 Generating Market Report...")
    report = analyzer.generate_market_report()
    
    print(f"\n📈 Total Jobs: {report['total_jobs']}")
    
    print("\n🔧 Top 10 In-Demand Skills:")
    for skill, count in report['top_skills'].items():
        bar = "█" * (count // 5)
        print(f"  {skill:20} {count:4} jobs {bar}")
    
    print("\n💰 Highest Paying Skills:")
    for skill, salary in report['highest_paying_skills'].items():
        print(f"  {skill:20} ${salary:,.0f} avg")
    
    print("\n💼 Salary by Experience:")
    for level, salary in report['salary_by_experience'].items():
        print(f"  {level:12} ${salary['min']:,.0f} - ${salary['max']:,.0f}")
    
    print("\n🏢 Top Hiring Companies:")
    for company, count in report['top_companies'].items():
        print(f"  {company:20} {count} openings")
    
    print("\n📍 Location Distribution:")
    for location, count in report['location_distribution'].items():
        print(f"  {location:20} {count} jobs")
    
    print("\n🏠 Remote Work Stats:")
    remote = report['remote_stats']
    print(f"  Remote: {remote['remote_percentage']}% ({remote['remote_count']} jobs)")
    print(f"  On-site: {remote['onsite_percentage']}% ({remote['onsite_count']} jobs)")
    
    # Test role comparison
    print("\n" + "="*60)
    print("⚔️ Role Comparison: Data Engineer vs ML Engineer")
    print("="*60)
    comparison = analyzer.get_role_comparison("Data Engineer", "Machine Learning")
    
    for key in ['role1', 'role2']:
        role = comparison[key]
        print(f"\n{role['role']}:")
        if role['count'] > 0:
            print(f"  Jobs: {role['count']}")
            print(f"  Salary: ${role['avg_salary_min']:,.0f} - ${role['avg_salary_max']:,.0f}")
            print(f"  Remote: {role['remote_percentage']}%")
            print(f"  Top Skills: {', '.join(role['top_skills'].keys())}")
        else:
            print("  No data available")
    
    print("\n" + "="*60)
    print("🎉 Analytics Engine test complete!")
    print("="*60)