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

# ── HELPERS ────────────────────────────────────────────────────────────


@st.cache_resource
def get_s3_client(key: str, secret: str):
    """Create and cache a boto3 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=AWS_REGION,
    )


@st.cache_data(show_spinner=False)
def fetch_customer_list(_s3):
    """List unique customer IDs under access_logs/ (Streamlit won’t hash `_s3`)."""
    paginator = _s3.get_paginator("list_objects_v2")
    customers = set()
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="access_logs/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) > 2:
                customers.add(parts[2])
    return sorted(customers)


# ── SIDEBAR ────────────────────────────────────────────────────────────
st.sidebar.header("AWS Credentials")
access_key = st.sidebar.text_input("Access Key ID",     type="password")
secret_key = st.sidebar.text_input("Secret Access Key", type="password")

if access_key and secret_key:
    try:
        s3 = get_s3_client(access_key, secret_key)
        # quick test
        s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=1)
        st.sidebar.success("Connected to S3")
    except Exception as e:
        st.sidebar.error(f"AWS Error: {e}")
        st.stop()
else:
    st.sidebar.info("Enter AWS creds above.")
    st.stop()

# ── MAIN UI ────────────────────────────────────────────────────────────
st.title("📊 App Usage Dashboard")

# Customer selector
customers = ["All"] + fetch_customer_list(s3)
cust = st.selectbox("Select Customer", customers)

# Date range selector (defaults to last 7 days)
today = datetime.date.today()
default_start = today - datetime.timedelta(days=7)
start_date, end_date = st.date_input(
    "Select date range",
    value=[default_start, today]
)

# Analyze button
if st.button("Analyze"):
    st.info(f"Scanning logs for '{cust}' from {start_date} to {end_date}…")
    counts = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="access_logs/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # only .txt logs
            if not key.endswith(".txt"):
                continue

            # extract date from filename
            # e.g. localhost_access_log.2025-04-25.txt
            filename = key.split("/")[-1]
            try:
                date_str = filename.split('.')[-2]
                log_date = datetime.datetime.strptime(
                    date_str, "%Y-%m-%d").date()
            except (IndexError, ValueError):
                continue

            # date filter
            if log_date < start_date or log_date > end_date:
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
        st.warning("No log entries found for that selection.")
    else:
        # build DataFrame
        df = pd.DataFrame({
            "Report": list(counts.keys()),
            "Calls":  list(counts.values())
        }).sort_values("Calls", ascending=False)

        # pie chart
        fig = px.pie(
            df, names="Report", values="Calls",
            title=f"Report Usage for '{cust}'",
            hole=0.3
        )
        st.plotly_chart(fig, use_container_width=True)

        # top-N table
        max_n = min(100, len(df))
        top_n = st.slider("Show top N reports", 5, max_n, 10)
        st.dataframe(df.head(top_n).reset_index(drop=True))
