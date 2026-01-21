"""
Vector Store for semantic job search using ChromaDB.

What is a Vector Store?
- It converts text into numbers (embeddings) that capture meaning
- Similar meanings = similar numbers
- So "Python developer" and "Python programmer" are seen as related!
"""
import chromadb
from chromadb.config import Settings
from typing import Optional
from pathlib import Path
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.data_collection.models import JobPosting


class JobVectorStore:
    """
    Stores job postings as vectors for semantic search.
    This enables AI-powered "smart" search that understands meaning!
    """
    
    def __init__(self, persist_dir: str = "data/chroma_db"):
        """
        Initialize the vector store.
        
        Args:
            persist_dir: Where to save the vector database
        """
        self.persist_dir = persist_dir
        Path(persist_dir).mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB with persistence
        self.client = chromadb.PersistentClient(path=persist_dir)
        
        # Create or get our jobs collection
        self.collection = self.client.get_or_create_collection(
            name="job_postings",
            metadata={"description": "Job postings for semantic search"}
        )
        
        print(f"✅ Vector store initialized with {self.collection.count()} documents")
    
    def add_job(self, job: JobPosting):
        """Add a single job to the vector store."""
        # Create a rich text document for embedding
        document = self._job_to_document(job)
        
        # Create metadata for filtering
        metadata = {
            "company": job.company,
            "location": job.location,
            "experience_level": job.experience_level,
            "remote": str(job.remote),
            "salary_min": job.salary_min or 0,
            "salary_max": job.salary_max or 0,
            "skills": ", ".join(job.skills[:10]),  # Top 10 skills
            "source": job.source
        }
        
        # Add to collection (ChromaDB handles embedding automatically!)
        self.collection.add(
            documents=[document],
            metadatas=[metadata],
            ids=[job.id]
        )
    
    def add_jobs(self, jobs: list[JobPosting], batch_size: int = 100):
        """
        Add multiple jobs efficiently in batches.
        
        Args:
            jobs: List of job postings to add
            batch_size: How many to process at once
        """
        total = len(jobs)
        added = 0
        skipped = 0
        
        # Get existing IDs to avoid duplicates
        existing_ids = set(self.collection.get()['ids'])
        
        # Also track IDs we're adding in this run to avoid duplicates within batch
        seen_ids = set()
        
        for i in range(0, total, batch_size):
            batch = jobs[i:i + batch_size]
            
            # Prepare batch data
            documents, metadatas, ids = [], [], []
            
            for job in batch:
                # Skip if already in DB or already seen in this run
                if job.id in existing_ids or job.id in seen_ids:
                    skipped += 1
                    continue
                
                seen_ids.add(job.id)
                documents.append(self._job_to_document(job))
                metadatas.append({
                    "company": job.company,
                    "location": job.location,
                    "experience_level": job.experience_level,
                    "remote": str(job.remote),
                    "salary_min": job.salary_min or 0,
                    "salary_max": job.salary_max or 0,
                    "skills": ", ".join(job.skills[:10]),
                    "source": job.source
                })
                ids.append(job.id)
            
            # Add batch to collection
            if documents:
                self.collection.add(
                    documents=documents,
                    metadatas=metadatas,
                    ids=ids
                )
                added += len(documents)
            
            print(f"  Progress: {min(i + batch_size, total)}/{total} jobs processed")
        
        print(f"✅ Added {added} jobs, skipped {skipped} duplicates")
        return added, skipped
    
    def search(
        self,
        query: str,
        n_results: int = 10,
        experience_level: Optional[str] = None,
        remote_only: bool = False,
        min_salary: Optional[float] = None
    ) -> list[dict]:
        """
        Semantic search for jobs!
        
        This is the magic - you can ask natural questions like:
        - "Machine learning jobs with Python"
        - "Remote data engineering positions"
        - "Entry level AI jobs"
        
        Args:
            query: Natural language search query
            n_results: How many results to return
            experience_level: Filter by level
            remote_only: Only show remote jobs
            min_salary: Minimum salary filter
            
        Returns:
            List of relevant jobs with similarity scores
        """
        # Build filter conditions using ChromaDB's $and operator for multiple filters
        where_conditions = None
        filters = []
        
        if experience_level:
            filters.append({"experience_level": {"$eq": experience_level}})
        
        if remote_only:
            filters.append({"remote": {"$eq": "True"}})
        
        # ChromaDB requires $and wrapper for multiple conditions
        if len(filters) == 1:
            where_conditions = filters[0]
        elif len(filters) > 1:
            where_conditions = {"$and": filters}
        
        # Perform semantic search
        results = self.collection.query(
            query_texts=[query],
            n_results=n_results * 2,  # Get extra to filter
            where=where_conditions,
            include=["documents", "metadatas", "distances"]
        )
        
        # Process results
        jobs = []
        for i, doc in enumerate(results['documents'][0]):
            metadata = results['metadatas'][0][i]
            distance = results['distances'][0][i]
            
            # Apply salary filter (can't do in ChromaDB query easily)
            if min_salary and metadata.get('salary_min', 0) < min_salary:
                continue
            
            # Convert distance to similarity score (0-1, higher is better)
            similarity = max(0, 1 - distance)
            
            jobs.append({
                "document": doc,
                "metadata": metadata,
                "similarity_score": round(similarity, 3),
                "id": results['ids'][0][i]
            })
        
        # Return top n results
        return jobs[:n_results]
    
    def search_by_skills(self, skills: list[str], n_results: int = 10) -> list[dict]:
        """Search for jobs that require specific skills."""
        query = f"Jobs requiring skills: {', '.join(skills)}"
        return self.search(query, n_results)
    
    def find_similar_jobs(self, job_id: str, n_results: int = 5) -> list[dict]:
        """Find jobs similar to a specific job."""
        # Get the original job's document
        result = self.collection.get(ids=[job_id], include=["documents"])
        
        if not result['documents']:
            return []
        
        # Search for similar
        return self.search(result['documents'][0], n_results + 1)[1:]  # Exclude itself
    
    def get_collection_stats(self) -> dict:
        """Get statistics about the vector store."""
        all_data = self.collection.get(include=["metadatas"])
        
        stats = {
            "total_documents": self.collection.count(),
            "companies": {},
            "experience_levels": {},
            "remote_count": 0
        }
        
        for metadata in all_data['metadatas']:
            # Count companies
            company = metadata.get('company', 'Unknown')
            stats['companies'][company] = stats['companies'].get(company, 0) + 1
            
            # Count experience levels
            level = metadata.get('experience_level', 'Unknown')
            stats['experience_levels'][level] = stats['experience_levels'].get(level, 0) + 1
            
            # Count remote
            if metadata.get('remote') == 'True':
                stats['remote_count'] += 1
        
        # Get top 10 companies
        stats['top_companies'] = dict(
            sorted(stats['companies'].items(), key=lambda x: x[1], reverse=True)[:10]
        )
        del stats['companies']
        
        return stats
    
    def _job_to_document(self, job: JobPosting) -> str:
        """
        Convert job posting to a searchable document.
        We combine all important info into one text.
        """
        parts = [
            f"Job Title: {job.title}",
            f"Company: {job.company}",
            f"Location: {job.location}",
            f"Experience Level: {job.experience_level}",
            f"Remote: {'Yes' if job.remote else 'No'}",
            f"Skills: {', '.join(job.skills)}",
            f"Description: {job.description}"
        ]
        
        if job.salary_min and job.salary_max:
            parts.insert(4, f"Salary: ${job.salary_min:,.0f} - ${job.salary_max:,.0f}")
        
        return "\n".join(parts)
    
    def clear(self):
        """Clear all data from the vector store."""
        self.client.delete_collection("job_postings")
        self.collection = self.client.create_collection(
            name="job_postings",
            metadata={"description": "Job postings for semantic search"}
        )
        print("🗑️ Vector store cleared")


# Test the vector store
if __name__ == "__main__":
    from src.data_collection.collectors import SampleDataCollector
    
    print("🚀 Testing Vector Store System\n")
    
    # Create vector store
    vs = JobVectorStore("data/chroma_db")
    
    # Generate sample jobs
    print("📥 Generating sample jobs...")
    collector = SampleDataCollector(num_jobs=200)
    jobs = list(collector.collect())
    
    # Add jobs to vector store
    print("\n📊 Adding jobs to vector store...")
    vs.add_jobs(jobs)
    
    # Test semantic search
    print("\n" + "="*50)
    print("🔍 Testing Semantic Search")
    print("="*50)
    
    queries = [
        "machine learning engineer with Python experience",
        "remote data engineering jobs",
        "entry level AI positions with LLM experience",
        "senior backend developer with cloud experience",
        "analytics jobs requiring SQL and Python"
    ]
    
    for query in queries:
        print(f"\n🔎 Query: '{query}'")
        results = vs.search(query, n_results=3)
        
        for i, result in enumerate(results, 1):
            meta = result['metadata']
            print(f"  {i}. {meta.get('company', 'Unknown')} - {meta.get('experience_level', '')} position")
            print(f"     📍 {meta.get('location', '')} | Score: {result['similarity_score']}")
            print(f"     🔧 Skills: {meta.get('skills', '')[:50]}...")
    
    # Test filtered search
    print("\n" + "="*50)
    print("🎯 Testing Filtered Search")
    print("="*50)
    
    print("\n🔎 Remote Senior positions:")
    results = vs.search("data engineer", n_results=3, experience_level="Senior", remote_only=True)
    for r in results:
        print(f"  - {r['metadata']['company']} | Remote: {r['metadata']['remote']}")
    
    # Show stats
    print("\n📊 Vector Store Statistics:")
    stats = vs.get_collection_stats()
    print(f"  Total documents: {stats['total_documents']}")
    print(f"  Remote jobs: {stats['remote_count']}")
    print(f"  Experience distribution: {stats['experience_levels']}")