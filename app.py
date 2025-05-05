# app.py
import re
import datetime
import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
from botocore.exceptions import ClientError

# ── CONFIG ─────────────────────────────────────────────────────────────
BUCKET_NAME = "data-eng-datalake-test"
AWS_REGION = "us-west-2"

st.set_page_config(page_title="App Usage Dashboard", layout="wide")

# ── CACHED HELPERS ─────────────────────────────────────────────────────


@st.cache_resource
def get_s3_client(key: str, secret: str):
    return boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=AWS_REGION,
    )


@st.cache_data(show_spinner=False)
def fetch_customer_list(_s3):
    paginator = _s3.get_paginator("list_objects_v2")
    custs = set()
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="access_logs/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) > 2:
                custs.add(parts[2])
    return sorted(custs)


# ── SIDEBAR ────────────────────────────────────────────────────────────
st.sidebar.header("AWS Credentials")
access_key = st.sidebar.text_input("Access Key ID",     type="password")
secret_key = st.sidebar.text_input("Secret Access Key", type="password")

if not (access_key and secret_key):
    st.sidebar.info("Enter AWS creds to continue.")
    st.stop()

try:
    s3 = get_s3_client(access_key, secret_key)
    s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=1)
    st.sidebar.success("✅ Connected")
except Exception as e:
    st.sidebar.error(f"AWS Error: {e}")
    st.stop()

# ── MAIN UI ────────────────────────────────────────────────────────────
st.title("📊 App Usage Dashboard")

# Customer + Date Range
customers = ["All"] + fetch_customer_list(s3)
cust = st.selectbox("Customer", customers)

today = datetime.date.today()
default_start = today - datetime.timedelta(days=7)
start_date, end_date = st.date_input(
    "Date range",
    value=[default_start, today]
)

# ANALYZE BUTTON
if st.button("Analyze"):
    st.session_state.pop("analysis_df", None)
    st.info(f"Analyzing '{cust}' from {start_date} to {end_date}…")
    counts = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="access_logs/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".txt"):
                continue

            # filename date filter
            try:
                date_str = key.rsplit(".", 2)[1]  # "2025-04-25"
                log_date = datetime.datetime.strptime(
                    date_str, "%Y-%m-%d").date()
            except Exception:
                continue
            if not (start_date <= log_date <= end_date):
                continue

            # customer filter
            parts = key.split("/")
            if cust != "All" and parts[2] != cust:
                continue

            # fetch & parse
            try:
                body = s3.get_object(Bucket=BUCKET_NAME, Key=key)[
                    "Body"].read().decode()
            except ClientError:
                continue
            for line in body.splitlines():
                m = re.search(r'"(?:GET|POST) (.*?) HTTP/1\.\d"', line)
                if not m:
                    continue
                seg = m.group(1).split("/")[-1].split("?")[0]
                counts[seg] = counts.get(seg, 0) + 1

    if not counts:
        st.warning("No entries found.")
    else:
        df = (
            pd.DataFrame.from_dict(counts, orient="index", columns=["Calls"])
              .rename_axis("Report")
              .reset_index()
              .sort_values("Calls", ascending=False)
        )
        # store for interactive filtering
        st.session_state.analysis_df = df
        st.session_state.analysis_title = f"'{cust}' {start_date}–{end_date}"

# ── INTERACTIVE RESULTS ─────────────────────────────────────────────────
if "analysis_df" in st.session_state:
    df = st.session_state.analysis_df
    title = st.session_state.analysis_title
    max_n = len(df)
    default = min(10, max_n)
    top_n = st.slider(
        "Top N reports",
        min_value=1,
        max_value=max_n,
        value=default,
        help="Filters both the pie chart and the table",
        key="top_n_slider"
    )

    df_top = df.head(top_n)

    # Pie chart of top N
    fig = px.pie(
        df_top,
        names="Report",
        values="Calls",
        title=f"Top {top_n} report usage for {title}",
        hole=0.3
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.dataframe(df_top.reset_index(drop=True), use_container_width=True)
