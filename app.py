import os, sys, uuid
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(
    page_title="German Energy Intelligence",
    page_icon="⚡", layout="wide",
    initial_sidebar_state="collapsed",
)

# Secrets bridge
try:
    for k in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
               "SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","GROQ_API_KEY"]:
        if k in st.secrets and not os.environ.get(k):
            os.environ[k] = st.secrets[k]
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agent"))
from energy_agent import EnergyAgent, AgentResponse
from query_templates import run_template

# CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
section[data-testid="stMain"] > div { background: #F7F9FC; }
.block-container { padding: 0 !important; max-width: 100% !important; }
#MainMenu, footer, header { display: none !important; }
div[data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: 600 !important; color: #0D1F3C !important; }
div[data-testid="stMetricLabel"] { font-size: 0.7rem !important; color: #8A98B8 !important; }
.stTabs [data-baseweb="tab-list"] { gap: 0; border-bottom: 1px solid #DDE1EC; background: transparent; }
.stTabs [data-baseweb="tab"] { padding: 0.5rem 1rem; font-size: 0.8rem; font-weight: 500; color: #5A6A8A; border-bottom: 2px solid transparent; }
.stTabs [aria-selected="true"] { color: #0B8C6E !important; border-bottom: 2px solid #0B8C6E !important; background: transparent !important; }
div[data-testid="stForm"] { border: none !important; padding: 0 !important; background: transparent !important; }
.stTextInput input { border-radius: 6px !important; border: 1.5px solid #DDE1EC !important; font-size: 0.875rem !important; }
.stTextInput input:focus { border-color: #0B8C6E !important; box-shadow: 0 0 0 3px rgba(11,140,110,0.1) !important; }
.stFormSubmitButton > button { background: #0D1F3C !important; color: white !important; border: none !important; border-radius: 6px !important; font-weight: 500 !important; font-size: 0.85rem !important; }
div[data-testid="column"] { padding: 0 !important; }
</style>
""", unsafe_allow_html=True)

# Session state
if "agent" not in st.session_state:
    st.session_state.agent = EnergyAgent(session_id=str(uuid.uuid4())[:8])
if "history" not in st.session_state:
    st.session_state.history = []

@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    try:
        fc, _, _ = run_template("FORECAST")
        an, _, _ = run_template("ANOMALIES")
        sh, _, _ = run_template("RENEWABLE_SHARE")
        return fc, an, sh
    except Exception as e:
        return None, None, None

fc_df, an_df, sh_df = load_data()

# Chart helpers
def chart_forecast(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["upper_mwh"], fill=None,
        mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["lower_mwh"], fill="tonexty",
        mode="lines", line=dict(width=0), fillcolor="rgba(11,140,110,0.1)",
        showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["predicted_mwh"],
        mode="lines+markers", line=dict(color="#0B8C6E", width=2.5),
        marker=dict(size=5, color="#0B8C6E"),
        hovertemplate="%{x}<br><b>%{y:,.0f} MWh</b><extra></extra>"))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0,r=0,t=8,b=0), height=240, showlegend=False,
        font=dict(family="Inter",size=11,color="#8A98B8"),
        xaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC",tickformat="%b %d"),
        yaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC",tickformat=",.0f"))
    return fig

def chart_share(df):
    fig = go.Figure(go.Bar(x=df["date"], y=df["renewable_pct"],
        marker_color="#0B8C6E", marker_line_width=0, opacity=0.7,
        hovertemplate="%{x}<br><b>%{y:.1f}%</b><extra></extra>"))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0,r=0,t=8,b=0), height=240, showlegend=False,
        font=dict(family="Inter",size=11,color="#8A98B8"),
        xaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC"),
        yaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC",ticksuffix="%"))
    return fig

def chart_response(r: AgentResponse):
    if r.chart_data is None or r.chart_type is None:
        return None
    df, cfg = r.chart_data, r.chart_config
    colors = ["#0B8C6E","#1E3A6E","#D97706","#DC2626"]
    base = dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0,r=0,t=32,b=0), height=260, showlegend=True,
        font=dict(family="Inter",size=11,color="#8A98B8"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        title=dict(text=cfg.get("title",""), font=dict(size=11,color="#8A98B8"),x=0),
        xaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC"),
        yaxis=dict(gridcolor="#EEF0F7",linecolor="#DDE1EC"))
    if r.chart_type == "line":
        fig = go.Figure()
        if cfg.get("lower") and cfg["lower"] in df.columns:
            fig.add_trace(go.Scatter(x=df[cfg["x"]], y=df[cfg["upper"]],
                fill=None, mode="lines", line=dict(width=0),
                showlegend=False, hoverinfo="skip"))
            fig.add_trace(go.Scatter(x=df[cfg["x"]], y=df[cfg["lower"]],
                fill="tonexty", mode="lines", line=dict(width=0),
                fillcolor="rgba(11,140,110,0.1)", showlegend=False, hoverinfo="skip"))
        if cfg.get("color") and cfg["color"] in df.columns:
            for i, g in enumerate(sorted(df[cfg["color"]].unique())):
                s = df[df[cfg["color"]]==g]
                fig.add_trace(go.Scatter(x=s[cfg["x"]], y=s[cfg["y"]],
                    mode="lines+markers", name=str(g),
                    line=dict(color=colors[i%len(colors)],width=2),
                    marker=dict(size=4)))
        else:
            fig.add_trace(go.Scatter(x=df[cfg["x"]], y=df[cfg["y"]],
                mode="lines+markers", line=dict(color="#0B8C6E",width=2),
                marker=dict(size=4), showlegend=False))
    elif r.chart_type == "bar":
        fig = go.Figure(go.Bar(x=df[cfg["x"]], y=df[cfg["y"]],
            marker_color="#0B8C6E", marker_line_width=0, opacity=0.8))
    elif r.chart_type == "bar_horizontal":
        fig = go.Figure(go.Bar(x=df[cfg["x"]], y=df[cfg["y"]],
            orientation="h", marker_color="#0B8C6E",
            marker_line_width=0, opacity=0.8))
    else:
        return None
    fig.update_layout(**base)
    return fig

# ── Nav bar ──
ren_pct = f"{float(sh_df.iloc[-1]['renewable_pct']):.1f}%" if sh_df is not None and not sh_df.empty else "—"
fc_val  = f"{float(fc_df.iloc[0]['predicted_mwh']):,.0f} MWh" if fc_df is not None and not fc_df.empty else "—"
an_cnt  = str(len(an_df)) if an_df is not None else "—"

st.markdown(f"""
<div style="background:#0D1F3C;padding:0.7rem 2rem;display:flex;
    align-items:center;justify-content:space-between">
  <div style="width:200px">
    <span style="font-size:0.7rem;color:#7B91B8">
        SMARD · Bundesnetzagentur</span>
  </div>
  <div style="text-align:center">
    <span style="font-size:1.1rem;font-weight:600;color:#fff;font-family:Inter,sans-serif">
        <img src="https://flagcdn.com/w20/de.png" width="20" height="14" 
        style="vertical-align:middle;margin-right:6px;border-radius:2px">
        German Energy Intelligence</span>
  </div>
  <div style="display:flex;gap:2rem;align-items:center">
    <span style="font-size:0.7rem;color:#7B91B8">
        <span style="display:inline-block;width:6px;height:6px;border-radius:50%;
            background:#0B8C6E;margin-right:4px;vertical-align:middle"></span>Live
    </span>
    <span style="font-size:0.7rem;color:#7B91B8">Renewable share
        <b style="color:#4DD9B3;margin-left:4px">{ren_pct}</b></span>
    <span style="font-size:0.7rem;color:#7B91B8">Tomorrow
        <b style="color:#4DD9B3;margin-left:4px">{fc_val}</b></span>
    <span style="font-size:0.7rem;color:#7B91B8">Anomalies (90d)
        <b style="color:#4DD9B3;margin-left:4px">{an_cnt}</b></span>
  </div>
  <div style="font-size:0.65rem;color:#4A5E80;line-height:1.6">
    Snowflake · Prophet · LLaMA 3.3<br>2017–present · Region: DE
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

# ── Main columns ──
left, right = st.columns([58, 42], gap="large")

# ── LEFT: Q&A ──
with left:
    st.markdown("""
    <div style="padding:0 1.5rem">
    <h2 style="font-size:1.1rem;font-weight:600;color:#0D1F3C;margin-bottom:0.25rem">
        Ask about the German electricity grid</h2>
    <p style="font-size:0.825rem;color:#5A6A8A;margin-bottom:1.25rem;line-height:1.6">
        Questions are answered using live data from Snowflake's Gold layer —
        renewable generation, demand, forecasts, and grid anomalies.</p>
    </div>
    """, unsafe_allow_html=True)

    if not st.session_state.history:
        st.markdown("<p style='font-size:0.75rem;font-weight:600;color:#8A98B8;"
                    "text-transform:uppercase;letter-spacing:0.08em;"
                    "padding:0 1.5rem'>Suggested questions</p>",
                    unsafe_allow_html=True)
        hints = [
            "What is the renewable energy forecast for the next 7 days?",
            "Were there any anomalies in the last 90 days?",
            "What was Germany's renewable share last month?",
            "Wie war der Anteil erneuerbarer Energien letzten Monat?",
            "Welche Energiequelle hat zuletzt am meisten Strom erzeugt?",
            "Wie hat sich die Stromnachfrage in letzter Zeit verändert?",
        ]
        c1, c2 = st.columns(2)
        for i, h in enumerate(hints):
            (c1 if i % 2 == 0 else c2).markdown(
                f"<div style='background:#fff;border:1px solid #DDE1EC;"
                f"border-radius:8px;padding:0.65rem 0.875rem;font-size:0.775rem;"
                f"color:#3D4F72;margin-bottom:0.5rem;line-height:1.45;"
                f"cursor:default'>{h}</div>",
                unsafe_allow_html=True)
    else:
        for entry in st.session_state.history:
            q, r = entry["question"], entry["response"]
            st.markdown(
                f"<div style='padding:0 1.5rem'>"
                f"<p style='font-size:0.65rem;font-weight:600;color:#8A98B8;"
                f"text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.2rem'>Question</p>"
                f"<p style='font-size:0.9rem;font-weight:500;color:#0D1F3C;"
                f"margin-bottom:0.75rem'>{q}</p></div>",
                unsafe_allow_html=True)
            if r.blocked:
                st.error(r.text)
            else:
                st.markdown(
                    f"<div style='margin:0 1.5rem;background:#fff;border:1px solid #DDE1EC;"
                    f"border-left:3px solid #0B8C6E;border-radius:0 8px 8px 0;"
                    f"padding:0.875rem 1.125rem;margin-bottom:0.75rem'>"
                    f"<div style='margin-bottom:0.4rem'>"
                    f"<span style='font-size:0.6rem;background:#E4F4EF;color:#0B8C6E;"
                    f"border-radius:4px;padding:2px 7px;font-weight:600;"
                    f"letter-spacing:0.05em'>{r.template_used or 'AGENT'}</span>"
                    f"<span style='font-size:0.6rem;color:#8A98B8;margin-left:0.5rem'>"
                    f"{r.latency_ms:.0f} ms</span></div>"
                    f"<div style='font-size:0.875rem;color:#3D4F72;line-height:1.65'>"
                    f"{r.text}</div></div>",
                    unsafe_allow_html=True)
                fig = chart_response(r)
                if fig:
                    with st.container():
                        st.markdown("<div style='padding:0 1.5rem'>", unsafe_allow_html=True)
                        st.plotly_chart(fig, use_container_width=True,
                                        config={"displayModeBar": False})
                        st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("<hr style='border:none;border-top:1px solid #EEF0F7;"
                        "margin:0.5rem 1.5rem 1rem'>", unsafe_allow_html=True)

    st.markdown("<div style='padding:0 1.5rem;margin-top:1rem'>", unsafe_allow_html=True)
    remaining = 20 - st.session_state.agent.question_count
    with st.form("qform", clear_on_submit=True):
        q = st.text_input("question", label_visibility="collapsed",
                          placeholder="Ask about renewable generation, demand, forecasts, anomalies...")
        col_btn, col_note = st.columns([2, 5])
        with col_btn:
            sub = st.form_submit_button("Ask", type="primary", use_container_width=True)
        with col_note:
            st.markdown(f"<p style='font-size:0.65rem;color:#8A98B8;padding-top:0.5rem'>"
                        f"{remaining} of 20 questions remaining</p>",
                        unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if sub and q.strip():
        with st.spinner("Retrieving data..."):
            resp = st.session_state.agent.ask(q.strip())
        st.session_state.history.append({"question": q.strip(), "response": resp})
        st.rerun()

# ── RIGHT: Data panel with tabs ──
with right:
    st.markdown("""
    <div style="background:#fff;border:1px solid #DDE1EC;border-radius:10px;
        padding:1.25rem;height:100%">
    """, unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Forecast", "Renewable share", "Anomalies"])

    with tab1:
        if fc_df is not None and not fc_df.empty:
            st.plotly_chart(chart_forecast(fc_df), use_container_width=True,
                            config={"displayModeBar": False})
            # Key metrics below chart
            if sh_df is not None and not sh_df.empty:
                last7 = sh_df.tail(7)
                m1, m2, m3 = st.columns(3)
                m1.metric("Renewable share",
                          f"{float(last7['renewable_pct'].mean()):.1f}%",
                          help="7-day average")
                m2.metric("Renewable gen",
                          f"{float(last7['renewable_mwh'].mean())/1000:.0f} GWh",
                          help="7-day average daily generation")
                m3.metric("Total gen",
                          f"{float(last7['total_mwh'].mean())/1000:.0f} GWh",
                          help="7-day average daily total")
            # Forecast table
            st.markdown("<p style='font-size:0.7rem;font-weight:600;color:#8A98B8;"
                        "text-transform:uppercase;letter-spacing:0.08em;"
                        "margin-top:0.75rem;margin-bottom:0.5rem'>14-day outlook</p>",
                        unsafe_allow_html=True)
            display = fc_df[["date","predicted_mwh","lower_mwh","upper_mwh"]].copy()
            display.columns = ["Date","Forecast (MWh)","Lower","Upper"]
            display["Forecast (MWh)"] = display["Forecast (MWh)"].apply(lambda x: f"{float(x):,.0f}")
            display["Lower"] = display["Lower"].apply(lambda x: f"{float(x):,.0f}")
            display["Upper"] = display["Upper"].apply(lambda x: f"{float(x):,.0f}")
            st.dataframe(display, use_container_width=True, hide_index=True, height=220)
        else:
            st.info("Forecast data unavailable")

    with tab2:
        if sh_df is not None and not sh_df.empty:
            st.plotly_chart(chart_share(sh_df), use_container_width=True,
                            config={"displayModeBar": False})
            st.markdown("<p style='font-size:0.7rem;font-weight:600;color:#8A98B8;"
                        "text-transform:uppercase;letter-spacing:0.08em;"
                        "margin-top:0.75rem;margin-bottom:0.5rem'>Daily breakdown</p>",
                        unsafe_allow_html=True)
            display = sh_df[["date","renewable_pct","renewable_mwh","total_mwh"]].copy()
            display.columns = ["Date","Share %","Renewable (MWh)","Total (MWh)"]
            display["Share %"] = display["Share %"].apply(lambda x: f"{float(x):.1f}%")
            display["Renewable (MWh)"] = display["Renewable (MWh)"].apply(lambda x: f"{float(x):,.0f}")
            display["Total (MWh)"] = display["Total (MWh)"].apply(lambda x: f"{float(x):,.0f}")
            st.dataframe(display, use_container_width=True, hide_index=True, height=240)
        else:
            st.info("Renewable share data unavailable")

    with tab3:
        if an_df is not None and not an_df.empty:
            st.markdown(f"<p style='font-size:0.825rem;color:#5A6A8A;margin-bottom:0.75rem'>"
                        f"{len(an_df)} anomalies detected in the last 90 days.</p>",
                        unsafe_allow_html=True)
            for _, row in an_df.iterrows():
                t = str(row.get("type","")).replace("_"," ")
                severity = float(row.get("severity", 0))
                color = "#0B8C6E" if "spike" in str(row.get("type","")) else "#D97706"
                actual = float(row.get("actual_mwh", 0))
                expected = float(row.get("expected_mwh", 0))
                st.markdown(
                    f"<div style='background:#F7F9FC;border:1px solid #DDE1EC;"
                    f"border-left:3px solid {color};border-radius:0 6px 6px 0;"
                    f"padding:0.5rem 0.875rem;margin-bottom:0.4rem'>"
                    f"<div style='display:flex;justify-content:space-between;"
                    f"align-items:center;margin-bottom:0.2rem'>"
                    f"<span style='font-size:0.75rem;font-weight:600;color:#1A2540'>"
                    f"{row.get('date','')}</span>"
                    f"<span style='font-size:0.7rem;font-weight:500;color:{color}'>"
                    f"{t}</span></div>"
                    f"<div style='font-size:0.68rem;color:#8A98B8'>"
                    f"Actual: {actual:,.0f} MWh · Expected: {expected:,.0f} MWh · "
                    f"Severity: {severity:.3f}</div>"
                    f"</div>",
                    unsafe_allow_html=True)
        else:
            st.success("No anomalies detected in the last 90 days.")

    st.markdown("</div>", unsafe_allow_html=True)
