import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)
from datetime import date, timedelta

st.set_page_config(page_title="Stock Market Dashboard", page_icon="📈", layout="wide")

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]


@st.cache_resource
def get_connection():
    pem_key = st.secrets["snowflake"]["private_key"].strip()
    if not pem_key.startswith("-----"):
        pem_key = f"-----BEGIN PRIVATE KEY-----\n{pem_key}\n-----END PRIVATE KEY-----"

    private_key_obj = load_pem_private_key(
        pem_key.encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    private_key_der = private_key_obj.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )

    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=private_key_der,
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        warehouse=st.secrets["snowflake"]["warehouse"],
    )


@st.cache_data(ttl=3600)
def load_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT
                p.DATE, p.OPEN, p.HIGH, p.LOW, p.CLOSE, p.VOLUME,
                i.SMA_20, i.SMA_50,
                i.RSI_14,
                i.MACD_LINE, i.MACD_SIGNAL, i.MACD_HISTOGRAM,
                i.BB_UPPER, i.BB_MIDDLE, i.BB_LOWER
            FROM FCT_STOCK_PRICES p
            JOIN FCT_TECHNICAL_INDICATORS i
                ON p.SYMBOL = i.SYMBOL AND p.DATE = i.DATE
            WHERE p.SYMBOL = %s
              AND p.DATE BETWEEN %s AND %s
            ORDER BY p.DATE
            """,
            (symbol, start_date, end_date),
        )
        df = cursor.fetch_pandas_all()
    finally:
        cursor.close()

    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.55, 0.225, 0.225],
        vertical_spacing=0.08,
        subplot_titles=(
            f"{symbol} — Price & Indicators",
            "RSI (14)",
            "MACD (12 / 26 / 9)",
        ),
    )

    # --- Row 1: Price + Bollinger Bands + SMAs ---
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["bb_upper"],
            line=dict(color="rgba(100,100,200,0.3)", width=1),
            name="BB Upper", showlegend=True,
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["bb_lower"],
            line=dict(color="rgba(100,100,200,0.3)", width=1),
            fill="tonexty",
            fillcolor="rgba(100,100,200,0.07)",
            name="BB Lower", showlegend=True,
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["bb_middle"],
            line=dict(color="rgba(100,100,200,0.5)", width=1, dash="dot"),
            name="BB Middle", showlegend=True,
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["close"],
            line=dict(color="#1f77b4", width=2),
            name="Close",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["sma_20"],
            line=dict(color="#ff7f0e", width=1.5, dash="dash"),
            name="SMA 20",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["sma_50"],
            line=dict(color="#2ca02c", width=1.5, dash="dash"),
            name="SMA 50",
        ),
        row=1, col=1,
    )

    # --- Row 2: RSI ---
    fig.add_hline(y=70, line=dict(color="red", dash="dot", width=1), row=2, col=1)
    fig.add_hline(y=30, line=dict(color="green", dash="dot", width=1), row=2, col=1)
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["rsi_14"],
            line=dict(color="#9467bd", width=2),
            name="RSI 14",
        ),
        row=2, col=1,
    )

    # --- Row 3: MACD ---
    colors = ["#d62728" if v < 0 else "#2ca02c" for v in df["macd_histogram"].fillna(0)]
    fig.add_trace(
        go.Bar(
            x=df["date"], y=df["macd_histogram"],
            marker_color=colors,
            name="MACD Histogram", opacity=0.6,
        ),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["macd_line"],
            line=dict(color="#1f77b4", width=1.5),
            name="MACD Line",
        ),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"], y=df["macd_signal"],
            line=dict(color="#ff7f0e", width=1.5),
            name="Signal Line",
        ),
        row=3, col=1,
    )

    fig.update_layout(
        height=850,
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis3=dict(title="Date"),
        yaxis=dict(title="Price (USD)"),
        yaxis2=dict(title="RSI", range=[0, 100]),
        yaxis3=dict(title="MACD"),
        hovermode="x unified",
    )

    return fig


# --- Sidebar ---
st.sidebar.title("Stock Market Dashboard")
st.sidebar.markdown("Daily OHLCV + Technical Indicators")
st.sidebar.divider()

symbol = st.sidebar.selectbox("Symbol", SYMBOLS)

end_date = date.today()

INTERVALS = {
    "1M":  timedelta(days=30),
    "3M":  timedelta(days=90),
    "6M":  timedelta(days=180),
    "1Y":  timedelta(days=365),
    "YTD": timedelta(days=(end_date - date(end_date.year, 1, 1)).days),
}

st.sidebar.markdown("**Date Range**")
selected_interval = st.sidebar.radio(
    "Date Range",
    options=list(INTERVALS.keys()),
    index=2,
    horizontal=True,
    label_visibility="collapsed",
)

start_date = end_date - INTERVALS[selected_interval]

st.sidebar.divider()
st.sidebar.caption("Data: Polygon.io via Databricks → Snowflake")

with st.spinner(f"Loading {symbol}..."):
    df = load_data(symbol, str(start_date), str(end_date))

if df.empty:
    st.warning(f"No data found for {symbol} between {start_date} and {end_date}.")
    st.stop()

# Metrics row
latest = df.iloc[-1]
prev = df.iloc[-2] if len(df) > 1 else latest
price_delta = latest["close"] - prev["close"]
pct_delta = (price_delta / prev["close"]) * 100 if prev["close"] else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Close", f"${latest['close']:.2f}", f"{price_delta:+.2f} ({pct_delta:+.2f}%)")
col2.metric("RSI (14)", f"{latest['rsi_14']:.1f}" if pd.notna(latest["rsi_14"]) else "—")
col3.metric("SMA 20", f"${latest['sma_20']:.2f}" if pd.notna(latest["sma_20"]) else "—")
col4.metric("SMA 50", f"${latest['sma_50']:.2f}" if pd.notna(latest["sma_50"]) else "—")
col5.metric("MACD", f"{latest['macd_line']:.4f}" if pd.notna(latest["macd_line"]) else "—")

st.plotly_chart(build_chart(df, symbol), use_container_width=True)
