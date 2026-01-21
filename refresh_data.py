"""
Data Refresh Script - Fetches fresh job data from real sources.
Run this periodically to keep your database updated!

Usage:
    python refresh_data.py          # Fetch real jobs
    python refresh_data.py --sample # Generate sample data instead
    python refresh_data.py --clear  # Clear all data first
"""
import argparse
from datetime import datetime

from src.data_collection.collectors import SampleDataCollector, DataCollectionManager
from src.data_collection.real_collectors import RemotiveCollector, ArbeitnowCollector
from src.etl.database import JobDatabase
from src.rag.vector_store import JobVectorStore


def refresh_with_real_data(clear_first: bool = False):
    """Fetch real jobs from APIs and update database."""
    print("🌐 Fetching REAL job data from free APIs...")
    print("="*60)
    
    # Initialize
    db = JobDatabase()
    vs = JobVectorStore()
    
    if clear_first:
        print("🗑️ Clearing existing data...")
        db.clear_all()
        vs.clear()
    
    # Collect from real sources
    manager = DataCollectionManager()
    manager.add_collector(RemotiveCollector(
        categories=["software-dev", "data", "devops", "qa"],
        limit_per_category=100
    ))
    manager.add_collector(ArbeitnowCollector(max_pages=5))
    
    jobs = manager.collect_all()
    
    if jobs:
        print(f"\n📥 Saving {len(jobs)} jobs to database...")
        inserted, skipped = db.insert_many(jobs)
        
        print(f"\n📥 Adding to vector store...")
        added, _ = vs.add_jobs(jobs)
        
        print(f"\n✅ Refresh complete!")
        print(f"   Database: {inserted} new jobs added")
        print(f"   Vector store: {added} new embeddings added")
        
        # Show stats
        stats = db.get_stats()
        print(f"\n📊 Total jobs in database: {stats['total_jobs']}")
    else:
        print("❌ No jobs fetched. Check your internet connection.")


def refresh_with_sample_data(num_jobs: int = 500, clear_first: bool = False):
    """Generate sample data for testing."""
    print(f"🎲 Generating {num_jobs} sample jobs...")
    print("="*60)
    
    # Initialize
    db = JobDatabase()
    vs = JobVectorStore()
    
    if clear_first:
        print("🗑️ Clearing existing data...")
        db.clear_all()
        vs.clear()
    
    # Generate sample data
    manager = DataCollectionManager()
    manager.add_collector(SampleDataCollector(num_jobs=num_jobs))
    
    jobs = manager.collect_all()
    
    print(f"\n📥 Saving to database...")
    inserted, skipped = db.insert_many(jobs)
    
    print(f"\n📥 Adding to vector store...")
    added, _ = vs.add_jobs(jobs)
    
    print(f"\n✅ Refresh complete!")
    print(f"   Database: {inserted} new jobs added")
    print(f"   Vector store: {added} new embeddings added")


def main():
    parser = argparse.ArgumentParser(description="Refresh job market data")
    parser.add_argument("--sample", action="store_true", help="Use sample data instead of real APIs")
    parser.add_argument("--clear", action="store_true", help="Clear existing data before refresh")
    parser.add_argument("--num", type=int, default=500, help="Number of sample jobs (only with --sample)")
    
    args = parser.parse_args()
    
    print(f"\n🕐 Data refresh started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if args.sample:
        refresh_with_sample_data(num_jobs=args.num, clear_first=args.clear)
    else:
        refresh_with_real_data(clear_first=args.clear)
    
    print(f"\n🕐 Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()