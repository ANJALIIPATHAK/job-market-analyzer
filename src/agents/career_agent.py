"""
AI Career Agent - Uses LLMs to provide personalized career advice.
Supports multiple providers: Groq (FREE!) or Anthropic (paid).
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.rag.vector_store import JobVectorStore
from src.etl.database import JobDatabase

# Load environment variables
load_dotenv()


class LLMProvider:
    """Base class for LLM providers."""
    def generate(self, system: str, user_message: str) -> str:
        raise NotImplementedError


class GroqProvider(LLMProvider):
    """FREE LLM provider using Groq API."""
    
    def __init__(self):
        from groq import Groq
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not found!")
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"  # Free and powerful!
        print(f"✅ Using Groq ({self.model}) - FREE!")
    
    def generate(self, system: str, user_message: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_message}
            ],
            max_tokens=1500,
            temperature=0.7
        )
        return response.choices[0].message.content


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider (paid)."""
    
    def __init__(self):
        from anthropic import Anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found!")
        self.client = Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"
        print(f"✅ Using Anthropic ({self.model})")
    
    def generate(self, system: str, user_message: str) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1500,
            system=system,
            messages=[{"role": "user", "content": user_message}]
        )
        return response.content[0].text


def get_llm_provider() -> LLMProvider:
    """Get the configured LLM provider."""
    provider = os.getenv("LLM_PROVIDER", "groq").lower()
    
    if provider == "groq":
        return GroqProvider()
    elif provider == "anthropic":
        return AnthropicProvider()
    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {provider}")


class CareerAgent:
    """
    AI-powered career advisor using RAG (Retrieval Augmented Generation).
    """
    
    def __init__(
        self,
        vector_store: Optional[JobVectorStore] = None,
        database: Optional[JobDatabase] = None,
        llm_provider: Optional[LLMProvider] = None
    ):
        # Initialize LLM
        self.llm = llm_provider or get_llm_provider()
        
        # Initialize data stores
        self.vector_store = vector_store or JobVectorStore()
        self.database = database or JobDatabase()
        
        # System prompt
        self.system_prompt = """You are an expert AI Career Advisor with deep knowledge of the tech job market. 

Your role is to help job seekers by:
1. Analyzing job market trends and data
2. Providing personalized career advice
3. Recommending skills to learn
4. Suggesting job opportunities that match their profile

You have access to a database of real job postings and can provide data-driven insights.

When responding:
- Be encouraging but realistic
- Provide specific, actionable advice
- Use data to back up your recommendations
- Format responses clearly with bullet points when listing items
- If you don't have enough data, be honest about it

Remember: Your goal is to help people advance their careers!"""

        print("✅ Career Agent initialized")
    
    def _get_relevant_jobs(self, query: str, n_results: int = 5) -> list[dict]:
        """Retrieve relevant jobs using semantic search."""
        return self.vector_store.search(query, n_results=n_results)
    
    def _get_market_stats(self) -> dict:
        """Get current job market statistics."""
        return self.database.get_stats()
    
    def _format_jobs_context(self, jobs: list[dict]) -> str:
        """Format job results for the AI context."""
        if not jobs:
            return "No relevant jobs found in the database."
        
        lines = ["Here are relevant job postings from our database:\n"]
        
        for i, job in enumerate(jobs, 1):
            meta = job['metadata']
            lines.append(f"{i}. **{meta.get('company', 'Unknown')}** - {meta.get('experience_level', '')} position")
            lines.append(f"   Location: {meta.get('location', 'Not specified')}")
            lines.append(f"   Skills: {meta.get('skills', 'Not specified')}")
            sal_min, sal_max = meta.get('salary_min', 0), meta.get('salary_max', 0)
            if sal_min and sal_max:
                lines.append(f"   Salary: ${sal_min:,.0f} - ${sal_max:,.0f}")
            lines.append(f"   Match Score: {job['similarity_score']:.1%}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_stats_context(self, stats: dict) -> str:
        """Format market statistics for the AI context."""
        lines = ["Current Job Market Statistics:\n"]
        lines.append(f"Total Jobs in Database: {stats.get('total_jobs', 0)}")
        
        if stats.get('top_skills'):
            top_5 = list(stats['top_skills'].items())[:5]
            skills_str = ", ".join([f"{s[0]} ({s[1]} jobs)" for s in top_5])
            lines.append(f"Top Skills in Demand: {skills_str}")
        
        if stats.get('top_companies'):
            top_5 = list(stats['top_companies'].items())[:5]
            companies_str = ", ".join([f"{c[0]} ({c[1]} openings)" for c in top_5])
            lines.append(f"Top Hiring Companies: {companies_str}")
        
        if stats.get('salary_by_experience'):
            lines.append("\nAverage Salaries by Experience:")
            for level, salary in stats['salary_by_experience'].items():
                lines.append(f"  - {level}: ${salary['min']:,.0f} - ${salary['max']:,.0f}")
        
        if stats.get('remote_distribution'):
            remote = stats['remote_distribution']
            total = remote.get('remote', 0) + remote.get('on_site', 0)
            if total > 0:
                pct = remote.get('remote', 0) / total * 100
                lines.append(f"\nRemote Jobs: {pct:.1f}% of all positions")
        
        return "\n".join(lines)
    
    def ask(self, question: str, include_jobs: bool = True, include_stats: bool = True) -> str:
        """Ask the career agent a question."""
        context_parts = []
        
        if include_jobs:
            relevant_jobs = self._get_relevant_jobs(question)
            context_parts.append(self._format_jobs_context(relevant_jobs))
        
        if include_stats:
            stats = self._get_market_stats()
            context_parts.append(self._format_stats_context(stats))
        
        context = "\n\n---\n\n".join(context_parts)
        
        user_message = f"""Based on the following job market data, please answer this question:

**Question:** {question}

---

**Available Data:**

{context}

---

Please provide helpful, data-driven career advice based on the above information."""

        try:
            return self.llm.generate(self.system_prompt, user_message)
        except Exception as e:
            return f"Sorry, I encountered an error: {str(e)}"
    
    def get_skill_recommendations(self, current_skills: list[str], target_role: str) -> str:
        """Get personalized skill recommendations."""
        question = f"""I currently have these skills: {', '.join(current_skills)}
        
I want to become a {target_role}. 

What additional skills should I learn? Please prioritize them by importance."""
        return self.ask(question)
    
    def analyze_job_market(self, role: str) -> str:
        """Get a comprehensive job market analysis for a specific role."""
        question = f"""Please provide a comprehensive job market analysis for {role} positions:

1. Current demand and trends
2. Required skills (must-have vs nice-to-have)
3. Salary ranges by experience level
4. Top hiring companies
5. Remote work opportunities
6. Career growth path"""
        return self.ask(question)
    
    def compare_roles(self, role1: str, role2: str) -> str:
        """Compare two career paths."""
        jobs1 = self._get_relevant_jobs(role1, n_results=10)
        jobs2 = self._get_relevant_jobs(role2, n_results=10)
        
        question = f"""Compare these two career paths:

**Option 1: {role1}**
{self._format_jobs_context(jobs1)}

**Option 2: {role2}**
{self._format_jobs_context(jobs2)}

Compare on: salary, job availability, skills, growth, remote options."""
        return self.ask(question, include_jobs=False, include_stats=True)


# Test the agent
if __name__ == "__main__":
    print("🚀 Testing Career Agent\n")
    print("="*60)
    
    try:
        agent = CareerAgent()
    except ValueError as e:
        print(f"❌ Error: {e}")
        print("\n💡 Setup Instructions:")
        print("1. Go to https://console.groq.com/ (FREE!)")
        print("2. Create an account and get an API key")
        print("3. Add it to your .env file: GROQ_API_KEY=your_key_here")
        exit(1)
    
    question = "What are the most in-demand skills for data engineers right now?"
    
    print(f"\n{'='*60}")
    print(f"Question: {question}")
    print("="*60)
    
    response = agent.ask(question)
    print(f"\n{response}")
    
    print("\n" + "="*60)
    print("🎉 Career Agent test complete!")
    print("="*60)