"""
Smart Job Market Analyzer - Streamlit Web Application
A beautiful interface for AI-powered career insights!
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dotenv import load_dotenv

from src.agents.career_agent import CareerAgent
from src.analytics.analyzer import JobAnalyzer
from src.rag.vector_store import JobVectorStore
from src.etl.database import JobDatabase

# Load environment
load_dotenv()

# Page config
st.set_page_config(
    page_title="Smart Job Market Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        background-color: #f0f2f6;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_components():
    """Load and cache expensive components."""
    db = JobDatabase()
    vs = JobVectorStore()
    analyzer = JobAnalyzer(db)
    agent = CareerAgent(vs, db)
    return db, vs, analyzer, agent


def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Smart Job Market Analyzer</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666;">AI-Powered Career Intelligence Platform</p>', unsafe_allow_html=True)
    
    # Load components
    with st.spinner("Loading AI components..."):
        db, vs, analyzer, agent = load_components()
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/career.png", width=80)
        st.title("Navigation")
        
        page = st.radio(
            "Choose a page:",
            ["🤖 AI Career Advisor", "📈 Market Analytics", "🔍 Job Search", "📋 Market Report"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        # Quick stats
        stats = db.get_stats()
        st.metric("Total Jobs", f"{stats.get('total_jobs', 0):,}")
        st.metric("Companies", len(stats.get('top_companies', {})))
        st.metric("Skills Tracked", len(stats.get('top_skills', {})))
        
        st.divider()
        st.caption("Built with ❤️ using Streamlit, ChromaDB & Groq")
    
    # Main content based on page selection
    if page == "🤖 AI Career Advisor":
        render_advisor_page(agent, analyzer)
    elif page == "📈 Market Analytics":
        render_analytics_page(analyzer)
    elif page == "🔍 Job Search":
        render_search_page(vs, db)
    elif page == "📋 Market Report":
        render_report_page(analyzer)


def render_advisor_page(agent, analyzer):
    """AI Career Advisor chatbot page."""
    st.header("🤖 AI Career Advisor")
    st.markdown("Ask me anything about the tech job market!")
    
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "user_input" not in st.session_state:
        st.session_state.user_input = ""
    
    # Instructions
    st.info("💡 **How to use:** Enter a job role (e.g., 'ML Engineer') and click a Quick Action, OR type a full question and press Enter.")
    
    # Text input for role or full question
    user_input = st.text_input(
        "Enter a job role OR type your full question:",
        placeholder="e.g., 'ML Engineer' or 'Should I learn RAG systems for AI jobs?'",
        key="main_input"
    )
    
    # Quick action buttons
    st.markdown("**Quick Actions** (uses your input above):")
    col1, col2, col3, col4 = st.columns(4)
    
    question_to_ask = None
    
    with col1:
        if st.button("💡 Skills", key="btn_skills", help="Get skill recommendations"):
            if user_input.strip():
                question_to_ask = f"What skills should I learn to become a {user_input}? What are the most important technical and soft skills needed?"
            else:
                st.warning("⚠️ Please enter a job role first!")
    
    with col2:
        if st.button("💰 Salary", key="btn_salary", help="Get salary information"):
            if user_input.strip():
                question_to_ask = f"What is the salary range for {user_input}? Break it down by experience level (entry, mid, senior)."
            else:
                st.warning("⚠️ Please enter a job role first!")
    
    with col3:
        if st.button("🏢 Companies", key="btn_companies", help="See hiring companies"):
            if user_input.strip():
                question_to_ask = f"Which companies are hiring {user_input}s? What are the top employers and what do they look for?"
            else:
                st.warning("⚠️ Please enter a job role first!")
    
    with col4:
        if st.button("📈 Trends", key="btn_trends", help="Get market trends"):
            if user_input.strip():
                question_to_ask = f"What are the current job market trends for {user_input}? Is demand growing? What's the future outlook?"
            else:
                st.warning("⚠️ Please enter a job role first!")
    
    # Submit full question button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🚀 Ask Full Question", type="primary", key="btn_ask"):
            if user_input.strip():
                question_to_ask = user_input
            else:
                st.warning("⚠️ Please enter a question first!")
    with col2:
        if st.session_state.messages:
            if st.button("🗑️ Clear Chat", key="btn_clear"):
                st.session_state.messages = []
                st.rerun()
    
    st.divider()
    
    # Process the question if one was set
    if question_to_ask:
        # Add user message to history
        st.session_state.messages.append({"role": "user", "content": question_to_ask})
        
        # Generate response
        with st.spinner("🔍 Analyzing job market data..."):
            response = agent.ask(question_to_ask)
        
        # Add assistant response to history
        st.session_state.messages.append({"role": "assistant", "content": response})
        
        # Rerun to show updated chat
        st.rerun()
    
    # Display chat history
    if st.session_state.messages:
        st.markdown("### 💬 Conversation")
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    else:
        # Show examples when no conversation yet
        st.markdown("### 💬 Examples")
        st.markdown("""
        **Option 1: Role + Quick Action**
        - Type `ML Engineer` → Click `💡 Skills` → Get skills for ML Engineers
        - Type `Data Scientist` → Click `💰 Salary` → Get salary info for Data Scientists
        
        **Option 2: Full Question**
        - Type `Should I learn RAG systems to become an AI Engineer?` → Click `🚀 Ask Full Question`
        - Type `Compare Data Engineer vs ML Engineer careers` → Click `🚀 Ask Full Question`
        - Type `I know Python and SQL, what should I learn next?` → Click `🚀 Ask Full Question`
        """)


def render_analytics_page(analyzer):
    """Market analytics with charts."""
    st.header("📈 Market Analytics")
    
    report = analyzer.generate_market_report()
    
    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Total Jobs", f"{report['total_jobs']:,}")
    with col2:
        remote_pct = report['remote_stats']['remote_percentage']
        st.metric("🏠 Remote Jobs", f"{remote_pct}%")
    with col3:
        top_skill = list(report['top_skills'].keys())[0] if report['top_skills'] else "N/A"
        st.metric("🔥 Top Skill", top_skill)
    with col4:
        top_company = list(report['top_companies'].keys())[0] if report['top_companies'] else "N/A"
        st.metric("🏢 Top Employer", top_company)
    
    st.divider()
    
    # Charts row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔧 Most In-Demand Skills")
        skills_df = pd.DataFrame(
            list(report['top_skills'].items()),
            columns=['Skill', 'Count']
        )
        fig = px.bar(
            skills_df, x='Count', y='Skill',
            orientation='h',
            color='Count',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=400, showlegend=False, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Highest Paying Skills")
        paying_skills = report['highest_paying_skills']
        if paying_skills:
            skills_salary_df = pd.DataFrame(
                list(paying_skills.items()),
                columns=['Skill', 'Avg Salary']
            )
            fig = px.bar(
                skills_salary_df, x='Avg Salary', y='Skill',
                orientation='h',
                color='Avg Salary',
                color_continuous_scale='Greens'
            )
            fig.update_layout(height=400, showlegend=False, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Charts row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💼 Salary by Experience")
        salary_data = report['salary_by_experience']
        if salary_data:
            levels = list(salary_data.keys())
            mins = [salary_data[l]['min'] for l in levels]
            maxs = [salary_data[l]['max'] for l in levels]
            
            fig = go.Figure()
            fig.add_trace(go.Bar(name='Min Salary', x=levels, y=mins, marker_color='#667eea'))
            fig.add_trace(go.Bar(name='Max Salary', x=levels, y=maxs, marker_color='#764ba2'))
            fig.update_layout(barmode='group', height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🏠 Remote vs On-Site")
        remote_stats = report['remote_stats']
        fig = px.pie(
            values=[remote_stats['remote_count'], remote_stats['onsite_count']],
            names=['Remote', 'On-Site'],
            color_discrete_sequence=['#667eea', '#764ba2']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Location chart
    st.subheader("📍 Jobs by Location")
    location_df = pd.DataFrame(
        list(report['location_distribution'].items()),
        columns=['Location', 'Count']
    )
    fig = px.bar(
        location_df, x='Location', y='Count',
        color='Count',
        color_continuous_scale='Blues'
    )
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)


def render_search_page(vs, db):
    """Semantic job search page."""
    st.header("🔍 Smart Job Search")
    st.markdown("Find jobs using natural language - our AI understands what you're looking for!")
    
    # Search inputs
    col1, col2 = st.columns([3, 1])
    with col1:
        query = st.text_input(
            "What kind of job are you looking for?",
            placeholder="e.g., 'ML engineer with Python and PyTorch experience'"
        )
    with col2:
        num_results = st.selectbox("Results", [5, 10, 20], index=1)
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        exp_level = st.selectbox(
            "Experience Level",
            ["All", "Entry", "Mid", "Senior", "Lead", "Principal"]
        )
    with col2:
        remote_only = st.checkbox("Remote Only")
    with col3:
        min_salary = st.number_input("Min Salary ($)", value=0, step=10000)
    
    if st.button("🔍 Search Jobs", type="primary", use_container_width=True):
        if query:
            with st.spinner("Searching..."):
                results = vs.search(
                    query,
                    n_results=num_results,
                    experience_level=exp_level if exp_level != "All" else None,
                    remote_only=remote_only
                )
                
                # Filter by salary if specified
                if min_salary > 0:
                    results = [r for r in results if r['metadata'].get('salary_min', 0) >= min_salary]
            
            st.success(f"Found {len(results)} matching jobs!")
            
            for i, job in enumerate(results, 1):
                meta = job['metadata']
                score = job['similarity_score']
                
                with st.expander(
                    f"**{i}. {meta.get('company', 'Unknown')}** - {meta.get('experience_level', '')} | "
                    f"Match: {score:.0%}",
                    expanded=(i <= 3)
                ):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**📍 Location:** {meta.get('location', 'Not specified')}")
                        st.markdown(f"**💼 Level:** {meta.get('experience_level', 'Not specified')}")
                        st.markdown(f"**🏠 Remote:** {'Yes' if meta.get('remote') == 'True' else 'No'}")
                    with col2:
                        sal_min = meta.get('salary_min', 0)
                        sal_max = meta.get('salary_max', 0)
                        if sal_min and sal_max:
                            st.markdown(f"**💰 Salary:** ${sal_min:,.0f} - ${sal_max:,.0f}")
                        st.markdown(f"**🎯 Match Score:** {score:.1%}")
                    
                    st.markdown("**🔧 Skills:**")
                    st.markdown(meta.get('skills', 'Not specified'))
        else:
            st.warning("Please enter a search query!")


def render_report_page(analyzer):
    """Full market report page."""
    st.header("📋 Complete Market Report")
    
    report = analyzer.generate_market_report()
    
    # Summary
    st.subheader("📊 Executive Summary")
    
    summary_text = f"""
    Based on analysis of **{report['total_jobs']:,} job postings**, here are the key insights:
    
    - **Top Skills in Demand:** {', '.join(list(report['top_skills'].keys())[:5])}
    - **Remote Work:** {report['remote_stats']['remote_percentage']}% of jobs offer remote options
    - **Top Hiring Companies:** {', '.join(list(report['top_companies'].keys())[:3])}
    - **Highest Paying Skill:** {list(report['highest_paying_skills'].keys())[0] if report['highest_paying_skills'] else 'N/A'}
    """
    st.markdown(summary_text)
    
    # Detailed sections
    tab1, tab2, tab3, tab4 = st.tabs(["💰 Salaries", "🔧 Skills", "🏢 Companies", "📍 Locations"])
    
    with tab1:
        st.subheader("Salary Analysis")
        
        # By experience
        st.markdown("**By Experience Level:**")
        for level, salary in report['salary_by_experience'].items():
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.markdown(f"**{level}**")
            with col2:
                st.markdown(f"${salary['min']:,.0f}")
            with col3:
                st.markdown(f"${salary['max']:,.0f}")
        
        st.divider()
        
        # By role
        st.markdown("**By Role:**")
        role_data = report['salary_by_role']
        for role, salary in role_data.items():
            if role != 'Other':
                st.markdown(f"- **{role}:** ${salary['salary_min']:,.0f} - ${salary['salary_max']:,.0f}")
    
    with tab2:
        st.subheader("Skills Analysis")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Most In-Demand:**")
            for skill, count in list(report['top_skills'].items())[:10]:
                st.markdown(f"- {skill}: {count} jobs")
        
        with col2:
            st.markdown("**Highest Paying:**")
            for skill, salary in list(report['highest_paying_skills'].items())[:10]:
                st.markdown(f"- {skill}: ${salary:,.0f}")
    
    with tab3:
        st.subheader("Company Analysis")
        st.markdown("**Top Hiring Companies:**")
        for company, count in report['top_companies'].items():
            st.markdown(f"- **{company}:** {count} openings")
    
    with tab4:
        st.subheader("Location Analysis")
        for location, count in report['location_distribution'].items():
            pct = count / report['total_jobs'] * 100
            st.markdown(f"- **{location}:** {count} jobs ({pct:.1f}%)")
    
    # Download report
    st.divider()
    st.download_button(
        "📥 Download Full Report (JSON)",
        data=str(report),
        file_name="job_market_report.json",
        mime="application/json"
    )


if __name__ == "__main__":
    main()