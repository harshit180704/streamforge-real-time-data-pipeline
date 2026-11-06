
import os
import socket
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


# ============================================================
# STREAMFORGE - REAL-TIME E-COMMERCE ANALYTICS DASHBOARD
# Kafka -> Spark Structured Streaming -> PostgreSQL -> Streamlit
# ============================================================

st.set_page_config(
    page_title="StreamForge | Analytics Console",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://streamforge:streamforge_dev@127.0.0.1:5433/streamforge",
)

KAFKA_HOST = os.getenv("KAFKA_HOST", "127.0.0.1")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "29092"))
LIVE_THRESHOLD_SECONDS = 60


# ============================================================
# CUSTOM DARK THEME
# ============================================================

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Manrope:wght@400;500;600;700;800&display=swap');

    :root {
        --bg: #080d18;
        --panel: #111a2b;
        --line: rgba(148,163,184,.16);
        --muted: #91a1ba;
        --text: #e8eef8;
        --cyan: #45d6e8;
        --green: #56d6a0;
    }

    .stApp {
        background: var(--bg);
        color: var(--text);
    }

    [data-testid="stHeader"] {
        background: rgba(8,13,24,.9);
    }

    [data-testid="stSidebar"] {
        background: #0d1524;
        border-right: 1px solid var(--line);
    }

    [data-testid="stMainBlockContainer"] {
        max-width: 1550px;
        padding-top: 2rem;
    }

    html, body, [class*="css"] {
        font-family: 'Manrope', sans-serif;
    }

    h1, h2, h3, p, label {
        color: var(--text);
    }

    .brand-kicker, .section-label {
        color: var(--cyan);
        font: 500 .72rem 'DM Mono', monospace;
        letter-spacing: .15em;
        text-transform: uppercase;
    }

    .hero-title {
        font-size: 2.5rem;
        font-weight: 800;
        letter-spacing: -.06em;
        color: #f3f7ff;
        margin: 0;
    }

    .hero-sub {
        color: var(--muted);
        margin-top: .35rem;
    }

    .metric-card {
        background: linear-gradient(145deg,#152238,#101827);
        border: 1px solid var(--line);
        border-radius: 15px;
        padding: 1.05rem 1.15rem;
        min-height: 122px;
    }

    .metric-label {
        color: var(--muted);
        font-size: .76rem;
        font-weight: 700;
    }

    .metric-value {
        color: #f3f7ff;
        font-size: 1.8rem;
        font-weight: 800;
        letter-spacing: -.04em;
        margin-top: .35rem;
    }

    .metric-foot {
        color: #8da0bb;
        font: .68rem 'DM Mono', monospace;
        margin-top: .2rem;
    }

    .status {
        display: inline-block;
        padding: .4rem .75rem;
        border-radius: 99px;
        font: 500 .72rem 'DM Mono', monospace;
        border: 1px solid;
        margin: .15rem .25rem .4rem 0;
    }

    .good {
        color: #56d6a0;
        background: rgba(86,214,160,.09);
        border-color: rgba(86,214,160,.35);
    }

    .warn {
        color: #f6b85f;
        background: rgba(246,184,95,.09);
        border-color: rgba(246,184,95,.35);
    }

    .bad {
        color: #ff8295;
        background: rgba(255,130,149,.09);
        border-color: rgba(255,130,149,.35);
    }

    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--line);
        border-radius: 12px;
    }

    .footer {
        color: #71829b;
        font: .68rem 'DM Mono', monospace;
        border-top: 1px solid var(--line);
        padding-top: 1rem;
        margin-top: 1.6rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATABASE CONNECTION AND DATA LOADING
# ============================================================

@st.cache_resource
def get_engine(database_url):
    return create_engine(
        database_url,
        pool_pre_ping=True,
    )


@st.cache_data(ttl=5)
def load_metrics(database_url):
    query = text(
        """
        SELECT
            window_start,
            window_end,
            event_type,
            region,
            event_count,
            gross_sales
        FROM streamforge_event_metrics
        ORDER BY window_start DESC
        """
    )

    with get_engine(database_url).connect() as connection:
        return pd.read_sql(query, connection)


# ============================================================
# INFRASTRUCTURE HEALTH CHECKS
# ============================================================

def check_tcp(host, port, timeout=1.5):
    try:
        with socket.create_connection(
            (host, port),
            timeout=timeout,
        ):
            return True
    except OSError:
        return False


def style_chart(fig, height=320):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Manrope",
            color="#a9b8ce",
            size=11,
        ),
        margin=dict(
            l=12,
            r=12,
            t=15,
            b=12,
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,.12)",
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hoverlabel=dict(
            bgcolor="#111a2b",
            bordercolor="#34445d",
            font=dict(color="#eef4ff"),
        ),
    )

    return fig


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown("### 📡 StreamForge")
    st.caption("ANALYTICS CONSOLE")

    st.divider()

    st.markdown("**Data source**")
    st.code(
        "streamforge_event_metrics",
        language="text",
    )

    refresh_seconds = st.selectbox(
        "Auto-refresh interval",
        options=[10, 30, 60],
        index=0,
    )

    if st.button(
        "↻ Refresh now",
        use_container_width=True,
    ):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.markdown("**Infrastructure connectivity**")

    kafka_reachable = check_tcp(
        KAFKA_HOST,
        KAFKA_PORT,
    )

    if kafka_reachable:
        st.markdown("🟢 Kafka broker: **reachable**")
    else:
        st.markdown("🔴 Kafka broker: **unreachable**")

    st.caption(
        f"TCP check: {KAFKA_HOST}:{KAFKA_PORT}"
    )

    st.caption(
        "Spark status is inferred from recent database output. "
        "This dashboard does not query a Spark health endpoint."
    )

    st.divider()

    st.caption(
        "QA_* regions are excluded from headline business metrics."
    )


# ============================================================
# MAIN HEADER
# ============================================================

st.markdown(
    '<div class="brand-kicker">REAL-TIME DATA PLATFORM / 01</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="hero-title">StreamForge</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="hero-sub">'
    'E-commerce event intelligence · '
    'Kafka → Spark Structured Streaming → PostgreSQL'
    '</div>',
    unsafe_allow_html=True,
)


# ============================================================
# DASHBOARD RENDERING
# ============================================================

@st.fragment(run_every=refresh_seconds)
def render_dashboard():

    # Check PostgreSQL and load data.
    try:
        with get_engine(DATABASE_URL).connect() as connection:
            connection.execute(text("SELECT 1"))

        raw = load_metrics(DATABASE_URL)

    except Exception as error:

        st.markdown(
            '<span class="status bad">● DATABASE DISCONNECTED</span>',
            unsafe_allow_html=True,
        )

        st.error(
            "Unable to read streamforge_event_metrics from PostgreSQL."
        )

        st.code(str(error))

        st.info(
            "Check that Docker PostgreSQL is published on host port "
            "5433 and DATABASE_URL uses 127.0.0.1:5433."
        )

        return

    if raw.empty:

        st.markdown(
            '<span class="status good">● POSTGRESQL CONNECTED</span>',
            unsafe_allow_html=True,
        )

        st.info(
            "PostgreSQL is connected, but no aggregation rows exist yet. "
            "Start the producer and wait for Spark windows to close."
        )

        return

    # Normalize database values.
    raw["window_start"] = pd.to_datetime(
        raw["window_start"],
        utc=True,
    )

    raw["window_end"] = pd.to_datetime(
        raw["window_end"],
        utc=True,
    )

    raw["event_count"] = pd.to_numeric(
        raw["event_count"],
        errors="coerce",
    ).fillna(0)

    raw["gross_sales"] = pd.to_numeric(
        raw["gross_sales"],
        errors="coerce",
    ).fillna(0.0)

    # Exclude QA data from business metrics.
    df = raw[
        ~raw["region"]
        .astype(str)
        .str.upper()
        .str.startswith("QA_")
    ].copy()

    if df.empty:
        df = raw.copy()

        st.warning(
            "Only QA-region data exists; showing it temporarily."
        )

    # Determine data freshness.
    latest_window = df["window_end"].max()

    age_seconds = max(
        0,
        int(
            (
                datetime.now(timezone.utc)
                - latest_window.to_pydatetime()
            ).total_seconds()
        ),
    )

    spark_recent = age_seconds <= LIVE_THRESHOLD_SECONDS

    # Infrastructure status.
    status_html = (
        '<span class="status good">● POSTGRESQL CONNECTED</span>'
    )

    if kafka_reachable:
        status_html += (
            '<span class="status good">● KAFKA REACHABLE</span>'
        )
    else:
        status_html += (
            '<span class="status bad">● KAFKA UNREACHABLE</span>'
        )

    if spark_recent:
        status_html += (
            '<span class="status good">● SPARK OUTPUT: RECENT</span>'
        )
    else:
        status_html += (
            '<span class="status warn">'
            '● SPARK OUTPUT: IDLE / HISTORICAL</span>'
        )

    st.markdown(
        status_html,
        unsafe_allow_html=True,
    )

    # ========================================================
    # KPI CALCULATIONS
    # ========================================================

    total_events = int(
        df["event_count"].sum()
    )

    total_sales = float(
        df["gross_sales"].sum()
    )

    purchase_events = int(
        df.loc[
            df["event_type"] == "purchase",
            "event_count",
        ].sum()
    )

    purchase_share = (
        purchase_events / total_events * 100
        if total_events
        else 0
    )

    active_regions = int(
        df["region"].nunique()
    )

    st.markdown(
        '<div class="section-label">PLATFORM OVERVIEW</div>',
        unsafe_allow_html=True,
    )

    kpis = [
        (
            "VALID EVENTS",
            f"{total_events:,}",
            "Accepted events in stored windows",
        ),
        (
            "GROSS SALES",
            f"₹{total_sales:,.2f}",
            "Purchase amount · INR",
        ),
        (
            "ACTIVE REGIONS",
            str(active_regions),
            "Distinct operating regions",
        ),
        (
            "PURCHASE SHARE",
            f"{purchase_share:.1f}%",
            f"{purchase_events:,} purchase events",
        ),
    ]

    columns = st.columns(4)

    for column, (label, value, footnote) in zip(columns, kpis):

        with column:

            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-foot">{footnote}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ========================================================
    # EVENT INTELLIGENCE
    # ========================================================

    st.markdown(
        '<div class="section-label">EVENT INTELLIGENCE</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns(2, gap="large")

    # Event type chart.
    with left:

        with st.container(border=True):

            st.markdown("**Event volume by type**")
            st.caption("Distribution of validated events")

            by_type = (
                df.groupby(
                    "event_type",
                    as_index=False,
                )["event_count"]
                .sum()
                .sort_values(
                    "event_count",
                    ascending=True,
                )
            )

            fig = px.bar(
                by_type,
                x="event_count",
                y="event_type",
                orientation="h",
                text="event_count",
                color_discrete_sequence=["#45d6e8"],
            )

            fig.update_traces(
                textposition="outside",
            )

            fig.update_layout(
                xaxis_title="Events",
                yaxis_title="",
            )

            st.plotly_chart(
                style_chart(fig, 330),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    # Regional activity chart.
    with right:

        with st.container(border=True):

            st.markdown("**Regional activity**")
            st.caption("Validated event count by region")

            by_region = (
                df.groupby(
                    "region",
                    as_index=False,
                )["event_count"]
                .sum()
                .sort_values(
                    "event_count",
                    ascending=False,
                )
            )

            fig = px.bar(
                by_region,
                x="region",
                y="event_count",
                text="event_count",
                color_discrete_sequence=["#8b7cf6"],
            )

            fig.update_traces(
                textposition="outside",
            )

            fig.update_layout(
                xaxis_title="",
                yaxis_title="Events",
            )

            st.plotly_chart(
                style_chart(fig, 330),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    # ========================================================
    # REVENUE PERFORMANCE
    # ========================================================

    st.markdown(
        '<div class="section-label">REVENUE PERFORMANCE</div>',
        unsafe_allow_html=True,
    )

    with st.container(border=True):

        st.markdown("**Gross sales by region**")
        st.caption("Aggregated purchase value · INR")

        sales_by_region = (
            df.groupby(
                "region",
                as_index=False,
            )["gross_sales"]
            .sum()
            .sort_values(
                "gross_sales",
                ascending=False,
            )
        )

        fig = px.bar(
            sales_by_region,
            x="region",
            y="gross_sales",
            text_auto=",.0f",
            color_discrete_sequence=["#56d6a0"],
        )

        fig.update_traces(
            textposition="outside",
            hovertemplate=(
                "%{x}<br>"
                "Gross sales: ₹%{y:,.2f}"
                "<extra></extra>"
            ),
        )

        fig.update_layout(
            xaxis_title="",
            yaxis_title="Gross sales (INR)",
        )

        st.plotly_chart(
            style_chart(fig, 300),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ========================================================
    # STREAMING ACTIVITY
    # ========================================================

    st.markdown(
        '<div class="section-label">STREAMING ACTIVITY</div>',
        unsafe_allow_html=True,
    )

    with st.container(border=True):

        st.markdown("**Events per aggregation window**")

        st.caption(
            "Historical event-time view; this is not "
            "events-per-second throughput."
        )

        timeline = (
            df.groupby(
                ["window_start", "event_type"],
                as_index=False,
            )["event_count"]
            .sum()
        )

        fig = px.line(
            timeline,
            x="window_start",
            y="event_count",
            color="event_type",
            markers=True,
            color_discrete_sequence=[
                "#45d6e8",
                "#8b7cf6",
                "#56d6a0",
                "#f6b85f",
                "#f07e9b",
            ],
        )

        fig.update_layout(
            xaxis_title="Window start (UTC)",
            yaxis_title="Events per 10-second window",
        )

        st.plotly_chart(
            style_chart(fig, 330),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ========================================================
    # LATEST AGGREGATIONS TABLE
    # ========================================================

    st.markdown(
        '<div class="section-label">LATEST AGGREGATIONS</div>',
        unsafe_allow_html=True,
    )

    recent = (
        df.sort_values(
            "window_start",
            ascending=False,
        )
        .head(100)
        .copy()
    )

    recent["window_start"] = (
        recent["window_start"]
        .dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    )

    recent["window_end"] = (
        recent["window_end"]
        .dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    )

    recent["gross_sales"] = (
        recent["gross_sales"]
        .map(lambda amount: f"₹{amount:,.2f}")
    )

    st.dataframe(
        recent,
        use_container_width=True,
        hide_index=True,
        height=360,
    )

    # ========================================================
    # FOOTER
    # ========================================================

    latest_ist = latest_window.tz_convert(
        "Asia/Kolkata"
    )

    freshness_label = (
        "RECENT" if spark_recent else "STALE"
    )

    st.markdown(
        f"""
        <div class="footer">
            LATEST WINDOW:
            {latest_ist.strftime("%d %b %Y, %I:%M:%S %p IST")}
            · DATA FRESHNESS: {freshness_label} ({age_seconds}s old)
            · ROWS LOADED: {len(raw):,}
            · AUTO-REFRESH: {refresh_seconds}s
            <br>
            STREAMFORGE / LOCAL DEVELOPMENT BUILD
            · SYNTHETIC EVENT DATA
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# START DASHBOARD
# ============================================================

render_dashboard()
