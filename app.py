"""
app.py — SMARD German Energy Intelligence Dashboard
Uses native Streamlit components for Streamlit Cloud compatibility.
"""

import os
import sys
import uuid
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="German Energy Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Secrets bridge — MUST come after set_page_config, before agent imports
try:
    for key in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","GROQ_API_KEY"]:
        val = st.secrets.get(key, "")
        if val and not os.environ.get(key):
            os.environ[key] = val
except Exception as e:
    st.warning(f"Secrets not loaded: {e}")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agent"))

from energy_agent import EnergyAgent, AgentResponse
from query_templates import run_template



# Minimal CSS — only what Streamlit Cloud reliably renders
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

section[data-testid="stMain"] { background: #F4F6FA; }
.block-container { padding: 1rem 2rem 2rem !important; max-width: 100% !important; }
#MainMenu, footer { display: none !important; }
</style>
""", unsafe_allow_html=True)

# Session state
if "agent" not in st.session_state:
    st.session_state.agent = EnergyAgent(session_id=str(uuid.uuid4())[:8])
if "history" not in st.session_state:
    st.session_state.history = []

@st.cache_data(ttl=3600, show_spinner=False)
def load_live_data():
    try:
        fc_df, _, _ = run_template("FORECAST")
        an_df, _, _ = run_template("ANOMALIES")
        sh_df, _, _ = run_template("RENEWABLE_SHARE")
        return fc_df, an_df, sh_df
    except Exception as e:
        st.error(f"Data load error: {e}")
        return None, None, None

fc_df, an_df, sh_df = load_live_data()

# Plotly chart builders
def chart_forecast(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["upper_mwh"], fill=None, mode="lines",
        line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["lower_mwh"], fill="tonexty", mode="lines",
        line=dict(width=0), fillcolor="rgba(11,140,110,0.12)",
        showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["predicted_mwh"],
        mode="lines+markers",
        line=dict(color="#0B8C6E", width=2),
        marker=dict(size=5, color="#0B8C6E"),
        name="Forecast MWh",
        hovertemplate="%{x}<br>%{y:,.0f} MWh<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=8, b=0), height=220,
        font=dict(family="Inter", size=11, color="#5A6A8A"),
        xaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC"),
        yaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        showlegend=False,
    )
    return fig

def chart_share(df):
    fig = go.Figure(go.Bar(
        x=df["date"], y=df["renewable_pct"],
        marker_color="#0B8C6E", marker_line_width=0, opacity=0.75,
        hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=8, b=0), height=180,
        font=dict(family="Inter", size=11, color="#5A6A8A"),
        xaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC"),
        yaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC",
                   ticksuffix="%"),
        showlegend=False,
    )
    return fig

def chart_response(response):
    if response.chart_data is None or response.chart_type is None:
        return None
    df = response.chart_data
    cfg = response.chart_config

    if response.chart_type == "line":
        fig = go.Figure()
        colors = ["#0B8C6E", "#1E3A6E", "#D97706", "#DC2626"]
        if "lower" in cfg and cfg.get("lower") and cfg["lower"] in df.columns:
            fig.add_trace(go.Scatter(
                x=df[cfg["x"]], y=df[cfg["upper"]], fill=None,
                mode="lines", line=dict(width=0),
                showlegend=False, hoverinfo="skip"))
            fig.add_trace(go.Scatter(
                x=df[cfg["x"]], y=df[cfg["lower"]], fill="tonexty",
                mode="lines", line=dict(width=0),
                fillcolor="rgba(11,140,110,0.1)",
                showlegend=False, hoverinfo="skip"))
        if "color" in cfg and cfg["color"] in df.columns:
            for i, grp in enumerate(sorted(df[cfg["color"]].unique())):
                sub = df[df[cfg["color"]] == grp]
                fig.add_trace(go.Scatter(
                    x=sub[cfg["x"]], y=sub[cfg["y"]],
                    mode="lines+markers", name=str(grp),
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=4)))
        else:
            fig.add_trace(go.Scatter(
                x=df[cfg["x"]], y=df[cfg["y"]],
                mode="lines+markers",
                line=dict(color="#0B8C6E", width=2),
                marker=dict(size=4)))

    elif response.chart_type == "bar":
        fig = go.Figure(go.Bar(
            x=df[cfg["x"]], y=df[cfg["y"]],
            marker_color="#0B8C6E", marker_line_width=0, opacity=0.8))

    elif response.chart_type == "bar_horizontal":
        fig = go.Figure(go.Bar(
            x=df[cfg["x"]], y=df[cfg["y"]],
            orientation="h",
            marker_color="#0B8C6E", marker_line_width=0, opacity=0.8))
    else:
        return None

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=32, b=0), height=260,
        font=dict(family="Inter", size=11, color="#5A6A8A"),
        title=dict(text=cfg.get("title",""), font=dict(size=11, color="#8A98B8"),
                   x=0, xanchor="left"),
        xaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC"),
        yaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    return fig

# ── Header ──
renewable_pct = f"{float(sh_df.iloc[-1].get('renewable_pct',0)):.1f}%" if sh_df is not None and not sh_df.empty else "—"
forecast_val  = f"{float(fc_df.iloc[0].get('predicted_mwh',0)):,.0f} MWh" if fc_df is not None and not fc_df.empty else "—"
anomaly_count = str(len(an_df)) if an_df is not None else "—"

st.markdown(f"""
<div style="background:#0D1F3C;padding:0.75rem 2rem;display:flex;
    align-items:center;justify-content:space-between;margin:-1rem -2rem 0;border-radius:0">
  <div>
    <span style="font-family:Inter,sans-serif;font-size:0.9rem;font-weight:600;
        color:#fff">German Energy Intelligence</span>
    <span style="font-size:0.7rem;color:#7B91B8;margin-left:0.75rem">
        SMARD / Bundesnetzagentur</span>
  </div>
  <div style="font-size:0.7rem;color:#7B91B8;text-align:right;line-height:1.6">
    Snowflake Gold Layer · Prophet · LLaMA 3.3<br>Data: 2017–present · Region: DE
  </div>
</div>

<div style="background:#fff;border-bottom:1px solid #DDE1EC;padding:0.5rem 2rem;
    display:flex;gap:2.5rem;align-items:center;margin:0 -2rem 1.5rem;flex-wrap:wrap">
  <span style="display:flex;align-items:center;gap:0.4rem">
    <span style="width:7px;height:7px;border-radius:50%;background:#0B8C6E;display:inline-block"></span>
    <span style="font-size:0.65rem;color:#8A98B8;font-weight:500;text-transform:uppercase;
        letter-spacing:0.08em">Live</span>
  </span>
  <span style="font-size:0.65rem;color:#8A98B8;text-transform:uppercase;
      letter-spacing:0.08em">Renewable share
    <strong style="font-family:'IBM Plex Mono',monospace;color:#0B8C6E;
        font-size:0.8rem;margin-left:0.3rem">{renewable_pct}</strong>
  </span>
  <span style="font-size:0.65rem;color:#8A98B8;text-transform:uppercase;
      letter-spacing:0.08em">Tomorrow forecast
    <strong style="font-family:'IBM Plex Mono',monospace;color:#0B8C6E;
        font-size:0.8rem;margin-left:0.3rem">{forecast_val}</strong>
  </span>
  <span style="font-size:0.65rem;color:#8A98B8;text-transform:uppercase;
      letter-spacing:0.08em">Anomalies (90d)
    <strong style="font-family:'IBM Plex Mono',monospace;color:#0B8C6E;
        font-size:0.8rem;margin-left:0.3rem">{anomaly_count}</strong>
  </span>
</div>
""", unsafe_allow_html=True)

# ── Two columns ──
col_left, col_right = st.columns([62, 38], gap="large")

# ── LEFT: Q&A ──
with col_left:
    if not st.session_state.history:
        st.markdown("### German Energy Intelligence")
        st.caption("Ask questions about renewable generation, electricity demand, "
                   "forecasts, and grid anomalies — grounded in live SMARD data.")
        st.markdown("**Try asking:**")
        hints = [
            "What is the renewable energy forecast for the next 7 days?",
            "Were there any anomalies in the last 90 days?",
            "What was Germany's renewable share last month?",
            "Which energy source generated the most power recently?",
            "How does renewable generation compare to last year?",
            "How has electricity demand changed recently?",
        ]
        c1, c2 = st.columns(2)
        for i, h in enumerate(hints):
            (c1 if i % 2 == 0 else c2).markdown(
                f"<div style='background:#fff;border:1px solid #DDE1EC;border-radius:6px;"
                f"padding:0.6rem 0.875rem;font-size:0.775rem;color:#3D4F72;"
                f"margin-bottom:0.4rem;line-height:1.4'>{h}</div>",
                unsafe_allow_html=True)
    else:
        for entry in st.session_state.history:
            q = entry["question"]
            r = entry["response"]

            st.markdown(
                f"<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                f"text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.2rem'>"
                f"Question</p>"
                f"<p style='font-size:0.9rem;font-weight:500;color:#1A2540;"
                f"margin-bottom:0.75rem'>{q}</p>",
                unsafe_allow_html=True)

            if r.blocked:
                st.error(r.text)
            else:
                st.markdown(
                    f"<div style='background:#fff;border:1px solid #DDE1EC;"
                    f"border-left:3px solid #0B8C6E;border-radius:0 6px 6px 0;"
                    f"padding:0.875rem 1.125rem;margin-bottom:0.5rem'>"
                    f"<div style='margin-bottom:0.4rem'>"
                    f"<span style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
                    f"background:#E4F4EF;color:#0B8C6E;border-radius:3px;padding:1px 6px;"
                    f"font-weight:500'>{r.template_used or 'AGENT'}</span>"
                    f"<span style='font-size:0.6rem;color:#8A98B8;margin-left:0.5rem'>"
                    f"{r.latency_ms:.0f} ms</span></div>"
                    f"<div style='font-size:0.875rem;color:#3D4F72;line-height:1.65'>"
                    f"{r.text}</div></div>",
                    unsafe_allow_html=True)

                fig = chart_response(r)
                if fig:
                    st.plotly_chart(fig, use_container_width=True,
                                    config={"displayModeBar": False})

            st.divider()

    # Input
    remaining = 20 - st.session_state.agent.question_count
    st.markdown("<p style='font-size:0.7rem;font-weight:500;color:#5A6A8A;"
                "margin-bottom:0.25rem'>Ask about the German electricity grid</p>",
                unsafe_allow_html=True)

    with st.form("query_form", clear_on_submit=True):
        question = st.text_input(
            label="question",
            label_visibility="collapsed",
            placeholder="e.g. What was the renewable share last month?",
        )
        submitted = st.form_submit_button("Ask", type="primary")

    st.caption(f"{remaining} of 20 questions remaining this session")

    if submitted and question.strip():
        with st.spinner("Retrieving data..."):
            response = st.session_state.agent.ask(question.strip())
        st.session_state.history.append({"question": question.strip(), "response": response})
        st.rerun()

# ── RIGHT: Live data ──
with col_right:
    st.markdown("<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                "text-transform:uppercase;letter-spacing:0.1em;"
                "border-bottom:1px solid #DDE1EC;padding-bottom:0.4rem;"
                "margin-bottom:0.75rem'>Renewable generation forecast</p>",
                unsafe_allow_html=True)
    if fc_df is not None and not fc_df.empty:
        st.plotly_chart(chart_forecast(fc_df), use_container_width=True,
                        config={"displayModeBar": False})
    else:
        st.caption("Forecast unavailable")

    if sh_df is not None and not sh_df.empty:
        last7 = sh_df.tail(7)
        avg_pct   = float(last7["renewable_pct"].mean())
        avg_ren   = float(last7["renewable_mwh"].mean())
        avg_total = float(last7["total_mwh"].mean())

        st.markdown("<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                    "text-transform:uppercase;letter-spacing:0.1em;"
                    "border-bottom:1px solid #DDE1EC;padding-bottom:0.4rem;"
                    "margin-top:1rem;margin-bottom:0.75rem'>Last 7 days — averages</p>",
                    unsafe_allow_html=True)

        m1, m2 = st.columns(2)
        m1.metric("Renewable share", f"{avg_pct:.1f}%")
        m2.metric("Renewable gen", f"{avg_ren/1000:.0f} GWh")
        m3, m4 = st.columns(2)
        m3.metric("Total generation", f"{avg_total/1000:.0f} GWh")
        m4.metric("Anomalies (90d)", anomaly_count)

        st.markdown("<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                    "text-transform:uppercase;letter-spacing:0.1em;"
                    "border-bottom:1px solid #DDE1EC;padding-bottom:0.4rem;"
                    "margin-top:1rem;margin-bottom:0.75rem'>Renewable share — 30-day trend</p>",
                    unsafe_allow_html=True)
        st.plotly_chart(chart_share(sh_df), use_container_width=True,
                        config={"displayModeBar": False})

    if an_df is not None and not an_df.empty:
        st.markdown("<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                    "text-transform:uppercase;letter-spacing:0.1em;"
                    "border-bottom:1px solid #DDE1EC;padding-bottom:0.4rem;"
                    "margin-top:1rem;margin-bottom:0.75rem'>Detected anomalies</p>",
                    unsafe_allow_html=True)
        for _, row in an_df.head(8).iterrows():
            t = str(row.get("type","")).replace("_"," ")
            color = "#0B8C6E" if "spike" in str(row.get("type","")) else "#D97706"
            st.markdown(
                f"<div style='background:#F4F6FA;border:1px solid #DDE1EC;"
                f"border-left:3px solid {color};border-radius:0 5px 5px 0;"
                f"padding:0.4rem 0.7rem;margin-bottom:0.3rem;"
                f"display:flex;justify-content:space-between;align-items:center'>"
                f"<span style='font-size:0.7rem;color:#5A6A8A;font-family:IBM Plex Mono,monospace'>"
                f"{row.get('date','')}</span>"
                f"<span style='font-size:0.7rem;font-weight:500;color:#3D4F72'>{t}</span>"
                f"</div>",
                unsafe_allow_html=True)
