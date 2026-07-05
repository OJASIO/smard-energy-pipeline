"""
app.py — SMARD German Energy Intelligence Dashboard
Day 6: Streamlit UI — light theme, professional, data-forward.
No chatbot framing. Feels like an energy company's internal tool.
"""

import os
import sys
import uuid
import plotly.graph_objects as go
import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agent"))

from energy_agent import EnergyAgent, AgentResponse
from query_templates import run_template

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="German Energy Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# CSS — light theme, professional, clean
# ---------------------------------------------------------------------------
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>

:root {
    --bg:        #F4F6FA;
    --surface:   #FFFFFF;
    --surface-2: #F0F2F7;
    --border:    #DDE1EC;
    --border-2:  #C8CEDF;
    --navy:      #0D1F3C;
    --navy-mid:  #1E3A6E;
    --teal:      #0B8C6E;
    --teal-light:#E4F4EF;
    --teal-mid:  #0FA882;
    --slate:     #5A6A8A;
    --slate-dim: #8A98B8;
    --text:      #1A2540;
    --text-2:    #3D4F72;
    --amber:     #D97706;
    --amber-bg:  #FEF3C7;
    --red:       #DC2626;
    --red-bg:    #FEE2E2;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'Inter', sans-serif;
}

.stApp { background: var(--bg) !important; font-family: var(--sans); color: var(--text); }
#MainMenu, footer, header { display: none !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }

/* ── Top nav ── */
.topnav {
    background: var(--navy);
    padding: 0 2rem;
    height: 52px;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.topnav-brand {
    font-family: var(--sans);
    font-size: 0.875rem;
    font-weight: 600;
    color: #FFFFFF;
    letter-spacing: 0.01em;
}
.topnav-sub {
    font-size: 0.7rem;
    font-weight: 400;
    color: #7B91B8;
    margin-left: 0.75rem;
}
.topnav-right {
    font-size: 0.7rem;
    color: #7B91B8;
    text-align: right;
    line-height: 1.5;
}

/* ── Status strip ── */
.statusbar {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 2rem;
    height: 44px;
    display: flex;
    align-items: center;
    gap: 2.5rem;
}
.stat-group { display: flex; align-items: center; gap: 0.5rem; }
.stat-label {
    font-size: 0.65rem;
    font-weight: 500;
    color: var(--slate-dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.stat-value {
    font-family: var(--mono);
    font-size: 0.8rem;
    font-weight: 500;
    color: var(--teal);
}
.live-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: var(--teal);
    display: inline-block;
}

/* ── Main grid ── */
.main-grid {
    display: grid;
    grid-template-columns: 1fr 360px;
    height: calc(100vh - 96px);
}

/* ── Left panel ── */
.left-panel {
    display: flex;
    flex-direction: column;
    background: var(--bg);
    border-right: 1px solid var(--border);
    overflow: hidden;
}
.chat-scroll {
    flex: 1;
    overflow-y: auto;
    padding: 1.5rem 2rem 1rem;
}
.input-area {
    background: var(--surface);
    border-top: 1px solid var(--border);
    padding: 1rem 2rem;
}

/* ── Q&A entries ── */
.q-entry { margin-bottom: 1.5rem; }
.q-label {
    font-size: 0.65rem;
    font-weight: 600;
    color: var(--slate-dim);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.3rem;
}
.q-text {
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--text);
    line-height: 1.4;
    margin-bottom: 0.75rem;
}
.a-block {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--teal);
    border-radius: 0 6px 6px 0;
    padding: 0.875rem 1.125rem;
}
.a-meta {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.5rem;
}
.a-tag {
    font-family: var(--mono);
    font-size: 0.6rem;
    background: var(--teal-light);
    color: var(--teal);
    border-radius: 3px;
    padding: 1px 6px;
    font-weight: 500;
}
.a-latency {
    font-size: 0.6rem;
    color: var(--slate-dim);
}
.a-text {
    font-size: 0.875rem;
    color: var(--text-2);
    line-height: 1.65;
}
.blocked-msg {
    background: var(--red-bg);
    border: 1px solid #FECACA;
    border-left: 3px solid var(--red);
    border-radius: 0 6px 6px 0;
    padding: 0.75rem 1rem;
    font-size: 0.8rem;
    color: var(--red);
}

/* ── Welcome state ── */
.welcome {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    padding: 2rem;
    text-align: center;
}
.welcome-title {
    font-size: 1.3rem;
    font-weight: 600;
    color: var(--navy);
    margin-bottom: 0.25rem;
}
.welcome-desc {
    font-size: 0.825rem;
    color: var(--slate);
    margin-bottom: 2rem;
    line-height: 1.6;
}
.hint-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
    max-width: 520px;
    width: 100%;
}
.hint-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.625rem 0.875rem;
    font-size: 0.775rem;
    color: var(--text-2);
    text-align: left;
    cursor: default;
    line-height: 1.4;
}
.hint-card:hover { border-color: var(--teal); color: var(--navy); }

/* ── Input field ── */
.input-label {
    font-size: 0.7rem;
    font-weight: 500;
    color: var(--slate);
    margin-bottom: 0.4rem;
}
.session-note {
    font-size: 0.65rem;
    color: var(--slate-dim);
    margin-top: 0.35rem;
}
.stTextInput input {
    background: var(--bg) !important;
    border: 1px solid var(--border-2) !important;
    border-radius: 5px !important;
    color: var(--text) !important;
    font-family: var(--sans) !important;
    font-size: 0.875rem !important;
    padding: 0.55rem 0.875rem !important;
}
.stTextInput input:focus {
    border-color: var(--teal) !important;
    box-shadow: 0 0 0 3px rgba(11,140,110,0.1) !important;
    outline: none !important;
}
.stTextInput input::placeholder { color: var(--slate-dim) !important; }
.stFormSubmitButton button {
    background: var(--navy) !important;
    color: #FFFFFF !important;
    border: none !important;
    border-radius: 5px !important;
    font-family: var(--sans) !important;
    font-size: 0.825rem !important;
    font-weight: 500 !important;
    padding: 0.5rem 1.25rem !important;
    cursor: pointer !important;
}
.stFormSubmitButton button:hover { background: var(--navy-mid) !important; }

/* ── Right panel ── */
.right-panel {
    background: var(--surface);
    border-left: 1px solid var(--border);
    overflow-y: auto;
    padding: 1.25rem;
}
.panel-section { margin-bottom: 1.75rem; }
.section-label {
    font-size: 0.65rem;
    font-weight: 600;
    color: var(--slate-dim);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.75rem;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
}
.metric-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
}
.metric-tile {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.6rem 0.75rem;
}
.tile-label {
    font-size: 0.6rem;
    font-weight: 500;
    color: var(--slate-dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.2rem;
}
.tile-value {
    font-family: var(--mono);
    font-size: 1.05rem;
    font-weight: 500;
    color: var(--navy);
}
.tile-unit {
    font-size: 0.6rem;
    color: var(--slate-dim);
    margin-left: 0.2rem;
}
.anomaly-item {
    background: var(--bg);
    border: 1px solid var(--border);
    border-left: 3px solid var(--amber);
    border-radius: 0 5px 5px 0;
    padding: 0.45rem 0.7rem;
    margin-bottom: 0.35rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.anomaly-item.spike { border-left-color: var(--teal); }
.anomaly-date { font-size: 0.7rem; color: var(--slate); font-family: var(--mono); }
.anomaly-type { font-size: 0.7rem; font-weight: 500; color: var(--text-2); }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "agent" not in st.session_state:
    st.session_state.agent = EnergyAgent(session_id=str(uuid.uuid4())[:8])
if "history" not in st.session_state:
    st.session_state.history = []


# ---------------------------------------------------------------------------
# Live data (cached per session)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def load_live_data():
    try:
        fc_df, _, _ = run_template("FORECAST")
        an_df, _, _ = run_template("ANOMALIES")
        sh_df, _, _ = run_template("RENEWABLE_SHARE")
        return fc_df, an_df, sh_df
    except Exception:
        return None, None, None


fc_df, an_df, sh_df = load_live_data()

# ---------------------------------------------------------------------------
# Plotly helpers — light theme
# ---------------------------------------------------------------------------
LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#5A6A8A", size=10),
    margin=dict(l=4, r=4, t=28, b=4),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    xaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC",
               tickfont=dict(size=9, color="#8A98B8")),
    yaxis=dict(gridcolor="#E8EBF4", linecolor="#DDE1EC",
               tickfont=dict(size=9, color="#8A98B8")),
)


def chart_forecast(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["upper_mwh"], fill=None, mode="lines",
        line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["lower_mwh"], fill="tonexty", mode="lines",
        line=dict(width=0), fillcolor="rgba(11,140,110,0.1)",
        showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["predicted_mwh"],
        mode="lines+markers",
        line=dict(color="#0B8C6E", width=2),
        marker=dict(size=4, color="#0B8C6E"),
        name="Forecast",
        hovertemplate="%{x}<br>%{y:,.0f} MWh<extra></extra>",
    ))
    fig.update_layout(**LAYOUT_BASE, title=dict(
        text="14-day renewable forecast (MWh/day)",
        font=dict(size=10, color="#8A98B8"), x=0, xanchor="left"))
    return fig


def chart_share(df):
    fig = go.Figure(go.Bar(
        x=df["date"], y=df["renewable_pct"],
        marker_color="#0B8C6E", marker_line_width=0, opacity=0.75,
        hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(**LAYOUT_BASE, title=dict(
        text="Daily renewable share % — last 30 days",
        font=dict(size=10, color="#8A98B8"), x=0, xanchor="left"))
    fig.update_yaxes(ticksuffix="%")
    return fig


def chart_response(response: AgentResponse):
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
                marker=dict(size=4, color="#0B8C6E")))
        fig.update_layout(**LAYOUT_BASE, title=dict(
            text=cfg.get("title", ""),
            font=dict(size=10, color="#8A98B8"), x=0, xanchor="left"))

    elif response.chart_type == "bar":
        fig = go.Figure(go.Bar(
            x=df[cfg["x"]], y=df[cfg["y"]],
            marker_color="#0B8C6E", marker_line_width=0, opacity=0.8))
        fig.update_layout(**LAYOUT_BASE, title=dict(
            text=cfg.get("title", ""),
            font=dict(size=10, color="#8A98B8"), x=0, xanchor="left"))

    elif response.chart_type == "bar_horizontal":
        fig = go.Figure(go.Bar(
            x=df[cfg["x"]], y=df[cfg["y"]],
            orientation="h",
            marker_color="#0B8C6E", marker_line_width=0, opacity=0.8))
        fig.update_layout(**LAYOUT_BASE, title=dict(
            text=cfg.get("title", ""),
            font=dict(size=10, color="#8A98B8"), x=0, xanchor="left"))
    else:
        return None

    return fig


# ---------------------------------------------------------------------------
# Top nav
# ---------------------------------------------------------------------------
renewable_pct = "—"
forecast_val  = "—"
anomaly_count = "—"

if sh_df is not None and not sh_df.empty:
    renewable_pct = f"{float(sh_df.iloc[-1].get('renewable_pct', 0)):.1f}%"
if fc_df is not None and not fc_df.empty:
    forecast_val = f"{float(fc_df.iloc[0].get('predicted_mwh', 0)):,.0f} MWh"
if an_df is not None:
    anomaly_count = str(len(an_df))

st.markdown(f"""
<div class="topnav">
    <div>
        <span class="topnav-brand">German Energy Intelligence</span>
        <span class="topnav-sub">SMARD / Bundesnetzagentur</span>
    </div>
    <div class="topnav-right">
        Snowflake Gold Layer · Prophet Forecasting · LLaMA 3.3<br>
        Data: 2017 – present · Region: DE
    </div>
</div>
<div class="statusbar">
    <div class="stat-group">
        <span class="live-dot"></span>
        <span class="stat-label">Live</span>
    </div>
    <div class="stat-group">
        <span class="stat-label">Renewable share</span>
        <span class="stat-value">{renewable_pct}</span>
    </div>
    <div class="stat-group">
        <span class="stat-label">Tomorrow forecast</span>
        <span class="stat-value">{forecast_val}</span>
    </div>
    <div class="stat-group">
        <span class="stat-label">Anomalies (90d)</span>
        <span class="stat-value">{anomaly_count}</span>
    </div>
    <div class="stat-group">
        <span class="stat-label">Region</span>
        <span class="stat-value">DE</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Two-column layout
# ---------------------------------------------------------------------------
col_left, col_right = st.columns([62, 38])

# ── LEFT: Q&A panel ──
with col_left:
    if not st.session_state.history:
        hints = [
            "What is the renewable energy forecast for the next 7 days?",
            "Were there any anomalies in the last 90 days?",
            "What was Germany's renewable share last month?",
            "Which energy source generated the most power recently?",
            "How does renewable generation compare to last year?",
            "How has electricity demand changed recently?",
        ]
        st.markdown(f"""
        <div class="welcome">
            <div class="welcome-title">German Energy Intelligence</div>
            <div class="welcome-desc">
                Ask questions about renewable generation, electricity demand,<br>
                forecasts, and grid anomalies — grounded in live SMARD data.
            </div>
            <div class="hint-grid">
                {"".join(f'<div class="hint-card">{h}</div>' for h in hints)}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for entry in st.session_state.history:
            q = entry["question"]
            r = entry["response"]
            st.markdown(f"""
            <div class="q-entry">
                <div class="q-label">Question</div>
                <div class="q-text">{q}</div>
            """, unsafe_allow_html=True)

            if r.blocked:
                st.markdown(f'<div class="blocked-msg">{r.text}</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="a-block">
                    <div class="a-meta">
                        <span class="a-tag">{r.template_used or 'AGENT'}</span>
                        <span class="a-latency">{r.latency_ms:.0f} ms</span>
                    </div>
                    <div class="a-text">{r.text}</div>
                </div>
                """, unsafe_allow_html=True)

                fig = chart_response(r)
                if fig:
                    st.plotly_chart(fig, use_container_width=True,
                                    config={"displayModeBar": False})

            st.markdown("</div>", unsafe_allow_html=True)

    # Input area
    remaining = 20 - st.session_state.agent.question_count
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    st.markdown("<div class='input-label'>Ask about the German electricity grid</div>",
                unsafe_allow_html=True)

    with st.form("query_form", clear_on_submit=True):
        question = st.text_input(
            label="question",
            label_visibility="collapsed",
            placeholder="e.g. What was the renewable share last month?",
        )
        submitted = st.form_submit_button("Ask", use_container_width=False)

    st.markdown(f"<div class='session-note'>{remaining} of 20 questions remaining this session</div>",
                unsafe_allow_html=True)

    if submitted and question.strip():
        with st.spinner("Retrieving data..."):
            response = st.session_state.agent.ask(question.strip())
        st.session_state.history.append({"question": question.strip(), "response": response})
        st.rerun()

# ── RIGHT: Live data panel ──
with col_right:
    # Forecast chart
    st.markdown("<div class='section-label'>Renewable generation forecast</div>",
                unsafe_allow_html=True)
    if fc_df is not None and not fc_df.empty:
        st.plotly_chart(chart_forecast(fc_df), use_container_width=True,
                        config={"displayModeBar": False})
    else:
        st.caption("Forecast data unavailable")

    # Key metrics
    if sh_df is not None and not sh_df.empty:
        last7 = sh_df.tail(7)
        avg_pct   = float(last7["renewable_pct"].mean())
        avg_ren   = float(last7["renewable_mwh"].mean())
        avg_total = float(last7["total_mwh"].mean())

        st.markdown("<div class='section-label' style='margin-top:1rem'>Last 7 days — averages</div>",
                    unsafe_allow_html=True)
        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-tile">
                <div class="tile-label">Renewable share</div>
                <div class="tile-value">{avg_pct:.1f}<span class="tile-unit">%</span></div>
            </div>
            <div class="metric-tile">
                <div class="tile-label">Renewable gen</div>
                <div class="tile-value">{avg_ren/1000:.0f}<span class="tile-unit">GWh/day</span></div>
            </div>
        </div>
        <div class="metric-row">
            <div class="metric-tile">
                <div class="tile-label">Total generation</div>
                <div class="tile-value">{avg_total/1000:.0f}<span class="tile-unit">GWh/day</span></div>
            </div>
            <div class="metric-tile">
                <div class="tile-label">Anomalies (90d)</div>
                <div class="tile-value">{anomaly_count}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Renewable share trend
    if sh_df is not None and not sh_df.empty:
        st.markdown("<div class='section-label' style='margin-top:1rem'>Renewable share — 30-day trend</div>",
                    unsafe_allow_html=True)
        st.plotly_chart(chart_share(sh_df), use_container_width=True,
                        config={"displayModeBar": False})

    # Recent anomalies
    if an_df is not None and not an_df.empty:
        st.markdown("<div class='section-label' style='margin-top:1rem'>Detected anomalies</div>",
                    unsafe_allow_html=True)
        for _, row in an_df.head(8).iterrows():
            t = str(row.get("type", "")).replace("_", " ")
            cls = "spike" if "spike" in str(row.get("type", "")) else ""
            st.markdown(f"""
            <div class="anomaly-item {cls}">
                <span class="anomaly-date">{row.get('date','')}</span>
                <span class="anomaly-type">{t}</span>
            </div>
            """, unsafe_allow_html=True)
