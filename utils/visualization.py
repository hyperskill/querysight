import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timedelta
from .models import (
    QueryPattern,
    DBTModel,
    AIRecommendation,
    AnalysisResult
)

def render_analysis_dashboard(result: AnalysisResult) -> None:
    """Render the main analysis dashboard"""
    st.header("📊 Analysis Dashboard")
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Query Patterns",
            len(result.query_patterns),
            "identified"
        )
    with col2:
        st.metric(
            "DBT Coverage",
            f"{result.model_coverage.get('covered', 0):.1f}%",
            "of tables"
        )
    with col3:
        st.metric(
            "Uncovered Tables",
            len(result.uncovered_tables),
            "need attention"
        )
    with col4:
        st.metric(
            "DBT Models",
            len(result.dbt_models),
            "total"
        )
    
    # Query Pattern Analysis
    st.subheader("🔍 Query Pattern Analysis")
    render_pattern_analysis(result.query_patterns)
    
    # Coverage Analysis
    st.subheader("📈 Coverage Analysis")
    render_coverage_analysis(result)
    
    # Model Dependency Graph
    st.subheader("🔗 Model Dependencies")
    render_dependency_graph(result.dbt_models)

def render_pattern_analysis(patterns: List[QueryPattern]) -> None:
    """Render query pattern analysis visualizations"""
    if not patterns:
        st.warning("No query patterns to analyze")
        return
        
    # Convert patterns to DataFrame
    df = pd.DataFrame([
        {
            "pattern_id": p.pattern_id,
            "model_name": p.model_name,
            "frequency": p.frequency,
            "avg_duration_ms": p.avg_duration_ms,
            "complexity_score": p.complexity_score,
            "users": len(p.users),
            "tables": len(p.tables_accessed)
        }
        for p in patterns
    ])
    
    # Pattern frequency vs duration scatter plot
    fig = px.scatter(
        df,
        x="frequency",
        y="avg_duration_ms",
        size="complexity_score",
        color="model_name",
        hover_data=["users", "tables"],
        title="Query Pattern Analysis"
    )
    st.plotly_chart(fig)
    
    # Top patterns table
    st.markdown("### Top Query Patterns")
    pattern_table = df.sort_values("frequency", ascending=False).head(10)
    st.dataframe(pattern_table)

def render_coverage_analysis(result: AnalysisResult) -> None:
    """Render coverage analysis visualizations"""
    # Coverage pie chart
    coverage_data = pd.DataFrame([
        {"status": "Covered", "percentage": result.model_coverage.get('covered', 0)},
        {"status": "Uncovered", "percentage": result.model_coverage.get('uncovered', 0)}
    ])
    
    fig = px.pie(
        coverage_data,
        values="percentage",
        names="status",
        title="Table Coverage Analysis",
        color_discrete_map={"Covered": "#00CC96", "Uncovered": "#EF553B"}
    )
    st.plotly_chart(fig)
    
    # Uncovered tables
    if result.uncovered_tables:
        st.markdown("### Uncovered Tables")
        st.markdown("These tables are frequently queried but not modeled in dbt:")
        for table in sorted(result.uncovered_tables):
            st.markdown(f"- `{table}`")

def render_dependency_graph(models: Dict[str, DBTModel]) -> None:
    """Render interactive model dependency graph"""
    import networkx as nx
    
    # Create graph
    G = nx.DiGraph()
    
    # Add nodes and edges
    for model_name, model in models.items():
        G.add_node(model_name, materialization=model.materialization)
        for dep in model.depends_on:
            G.add_edge(dep, model_name)
    
    # Calculate node positions
    pos = nx.spring_layout(G)
    
    # Create plotly figure
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )
    
    # Add edges
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_trace['x'] += (x0, x1, None)
        edge_trace['y'] += (y0, y1, None)
    
    # Create node trace
    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers+text',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            )
        )
    )
    
    # Add nodes
    for node in G.nodes():
        x, y = pos[node]
        node_trace['x'] += (x,)
        node_trace['y'] += (y,)
        node_trace['text'] += (node,)
    
    # Create figure
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title='Model Dependency Graph',
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    )
    
    st.plotly_chart(fig)

def render_recommendations(recommendations: List[AIRecommendation]) -> None:
    """Render AI recommendations with interactive elements"""
    if not recommendations:
        st.warning("No recommendations available")
        return
        
    st.header("🤖 AI Recommendations")
    
    # Sort recommendations by priority score
    sorted_recs = sorted(
        recommendations,
        key=lambda x: x.priority_score,
        reverse=True
    )
    
    for rec in sorted_recs:
        with st.expander(f"{rec.suggestion_type.title()}: {rec.description[:100]}..."):
            # Metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Impact Score",
                    f"{rec.impact_score * 100:.1f}%",
                    "estimated improvement"
                )
            with col2:
                st.metric(
                    "Difficulty",
                    f"{rec.implementation_difficulty * 100:.1f}%",
                    "complexity"
                )
            with col3:
                st.metric(
                    "Priority Score",
                    f"{rec.priority_score * 100:.1f}",
                    "weighted score"
                )
            
            # Detailed information
            st.markdown("#### Details")
            st.markdown(rec.description)
            
            if rec.suggested_sql:
                st.markdown("#### Suggested SQL")
                st.code(rec.suggested_sql, language="sql")
            
            if rec.affected_models:
                st.markdown("#### Affected Models")
                for model in sorted(rec.affected_models):
                    st.markdown(f"- `{model}`")
            
            # Implementation status
            st.markdown("#### Status")
            new_status = st.selectbox(
                "Implementation Status",
                ["pending", "approved", "implemented", "rejected"],
                index=["pending", "approved", "implemented", "rejected"].index(rec.status)
            )
            
            if new_status != rec.status:
                # Here you would typically update the status in your storage
                rec.status = new_status
                st.success(f"Status updated to: {new_status}")
