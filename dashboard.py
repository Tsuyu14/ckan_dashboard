import streamlit as st
import requests
import pandas as pd
import altair as alt
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
CKAN_URL = "https://gdcatalognhic.nha.co.th"  # no trailing slash
API_KEY = os.getenv("CKAN_API_KEY")
headers = {"Authorization": API_KEY}

# === MANUAL CACHE REFRESH ===
if "refresh_cache" not in st.session_state:
    st.session_state.refresh_cache = False

if st.button("🔄 Refresh Dataset Cache"):
    st.session_state.refresh_cache = True

# === PAGE SETUP ===
st.set_page_config("CKAN Dashboard", layout="wide")
st.title("🧭 NHIC CKAN Monitoring Dashboard")
st.markdown("Connected to: " + CKAN_URL)

# === API WRAPPERS ===
@st.cache_data(ttl=600)
def get_organizations():
    r = requests.get(f"{CKAN_URL}/api/3/action/organization_list", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return []

@st.cache_data(ttl=600)
def get_datasets():
    r = requests.get(f"{CKAN_URL}/api/3/action/package_list", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return []

@st.cache_data(ttl=600)     #cache dataset_details
def get_dataset_detail(dataset_id):
    r = requests.get(f"{CKAN_URL}/api/3/action/package_show?id={dataset_id}", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return None

@st.cache_data(ttl=600)     #cache org_datails
def get_org_detail(org_id):
    r = requests.get(f"{CKAN_URL}/api/3/action/organization_show?id={org_id}", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return None

@st.cache_data(ttl=3600)    #cache search_datasets
def get_dataset_count(): # Always get the total first
    url = f"{CKAN_URL}/api/3/action/package_search"
    params = {"rows": 0}  # We just want metadata
    r = requests.get(url, headers=headers, params=params, timeout=10)
    if r.status_code == 200:
        return r.json()["result"]["count"]
    return 0
def _search_datasets_paginated_reliable(limit=10000):
    url = f"{CKAN_URL}/api/3/action/package_search"
    rows_per_page = 1000
    all_results = []
    total_count = get_dataset_count()
    st.write(f"📦 Total datasets from CKAN: {total_count:,}")
    total_pages = (min(limit, total_count) + rows_per_page - 1) // rows_per_page

    progress = st.progress(0, text="🔍 Loading datasets...")
    log_area = st.empty()

    for page in range(total_pages):
        offset = page * rows_per_page
        success = False
        retries = 3

        for attempt in range(1, retries + 1):
            try:
                params = {"rows": rows_per_page, "start": offset}
                r = requests.get(url, headers=headers, params=params, timeout=15)
                if r.status_code == 200:
                    results = r.json()["result"].get("results", [])
                    all_results.extend(results)
                    success = True
                    break
                else:
                    log_area.warning(f"⚠️ Page {page+1} attempt {attempt}: HTTP {r.status_code}")
            except Exception as e:
                log_area.warning(f"⚠️ Page {page+1} attempt {attempt}: {e}")

        progress.progress((page + 1) / total_pages, text=f"🔍 Loaded page {page+1}/{total_pages}")

        if not success:
            st.error(f"❌ Failed to fetch page {page+1} after {retries} attempts.")
            break
        else:
            log_area.empty() 
    progress.empty()
    return all_results

def search_datasets_paginated(limit=10000):
    cache_file = "dataset_cache.json"

    if st.session_state.refresh_cache:
        st.session_state.refresh_cache = False
        if os.path.exists(cache_file):
            os.remove(cache_file)
        st.cache_data.clear()

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            os.remove(cache_file)

    data = _search_datasets_paginated_reliable(limit=limit)
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except:
        pass

    return data if data else []

@st.cache_data(ttl=3600)
def get_all_org_details_parallel(max_threads=20):
    org_ids = get_organizations()
    org_details = []

    def fetch_detail(org_id):
        try:
            r = requests.get(f"{CKAN_URL}/api/3/action/organization_show?id={org_id}", headers=headers, timeout=8)
            if r.status_code == 200:
                return r.json()["result"]
        except:
            return None
        return None

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(fetch_detail, oid): oid for oid in org_ids}
        for future in as_completed(futures):
            result = future.result()
            if result:
                org_details.append(result)

    # 🔧 fallback: ensure every org at least has id & title
    fetched_ids = {org["name"] for org in org_details if "name" in org}
    for oid in org_ids:
        if oid not in fetched_ids:
            org_details.append({"name": oid, "title": oid})  # fallback stub

    return org_details


# === DATA LOADING ===
datasets = get_datasets()
orgs = get_organizations()

# Build dataset details + index
with st.spinner("🔄 Fetching dataset list from CKAN..."):
    dataset_data = search_datasets_paginated(limit=10000)
st.success(f"✅ Loaded {len(dataset_data):,} datasets.")
if not dataset_data:
    st.error("❌ No datasets returned. Please try refreshing or check your CKAN connection.")
    st.stop()  # Halts Streamlit execution safely
df_datasets = pd.DataFrame([{
    "title": d.get("title"),
    "name": d.get("name"),
    "organization": d.get("organization", {}).get("title", "—"),
    "org_id": d.get("organization", {}).get("name", "unknown"),
    "resources": len(d.get("resources", [])),
    "last_modified": d.get("metadata_modified", "—"),
    "views": d.get("tracking_summary", {}).get("total", 0)
} for d in dataset_data])

# Build org summary
org_dataset_count = df_datasets["organization"].value_counts().to_dict()
total_views = df_datasets["views"].sum()

# === TABS ===
tab1, tab2 = st.tabs(["📊 Overview", "🔍 Explorer"])

# === TAB 1: OVERVIEW ===
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🏢 Organizations", len(orgs))
    col2.metric("📦 Datasets (visible)", len(dataset_data))
    col3.metric("📦 Datasets (total)", len(datasets))
    col4.metric("👁️ Website Views (tracked)", total_views)

    sort_option = st.selectbox(
        "Sort datasets by organization",
        ["Most datasets", "Alphabetical A-Z", "Alphabetical Z-A"]
    )

    st.markdown("### 📊 Datasets by Organization")

    chart_data = pd.DataFrame({
        "Organization": list(org_dataset_count.keys()),
        "Datasets": list(org_dataset_count.values())
    })
    if sort_option == "Alphabetical A-Z":
        chart_data = chart_data.sort_values(by="Organization")
    elif sort_option == "Alphabetical Z-A":
        chart_data = chart_data.sort_values(by="Organization", ascending=False)
    else:
        chart_data = chart_data.sort_values(by="Datasets", ascending=False)

    #st.bar_chart(chart_data.set_index("Organization"))
    bar_chart = alt.Chart(chart_data).mark_bar().encode(
        x=alt.X("Organization", sort="x" if "Alphabetical" in sort_option else "-y"),
        y="Datasets"
    ).properties(width="container")
    st.altair_chart(bar_chart, use_container_width=True)

    st.markdown("### 🏷️ Top Tags")
    all_tags = []
    all_formats = []

    for d in dataset_data:
        tags = d.get("tags", [])
        all_tags += [t["name"] for t in tags if "name" in t]
    
        resources = d.get("resources", [])
        all_formats += [r.get("format", "unknown").upper() for r in resources]

    # Count top 10 tags
    tag_series = pd.Series(all_tags).value_counts().head(10)
    format_series = pd.Series(all_formats).value_counts().head(10)

    col_tag, col_format = st.columns(2)

    tag_df = tag_series.reset_index()
    tag_df.columns = ["Tag", "Count"]

    tag_chart = alt.Chart(tag_df).mark_bar().encode(
        x=alt.X("Tag", sort="-y"),
        y="Count"
    ).properties(height=300)

    with col_tag:
        st.altair_chart(tag_chart, use_container_width=True)
        st.caption("Top 10 Tags")

    format_df = format_series.reset_index()
    format_df.columns = ["Format", "Count"]

    format_chart = alt.Chart(format_df).mark_bar().encode(
        x=alt.X("Format", sort="-y"),
        y="Count"
    ).properties(height=300)

    with col_format:
        st.altair_chart(format_chart, use_container_width=True)
        st.caption("Top 10 Resource Formats")


# === TAB 2: EXPLORER ===
with tab2:
    st.markdown("### 🔍 Dataset Explorer")

    search = st.text_input("🔎 Search datasets or organizations", "")
    all_orgs = get_all_org_details_parallel()
    org_titles = [org["title"] for org in all_orgs]
    org_id_map = {org["title"]: org["name"] for org in all_orgs}
 
    org_filter = st.selectbox(
    f"🏢 Filter by organization (Total: {len(org_titles)})",
    ["All"] + sorted(org_titles)
    ) 
    filtered_df = df_datasets.copy()
    
    if org_filter != "All":
        org_id = org_id_map[org_filter]
        filtered_df = filtered_df[filtered_df["org_id"] == org_id]

    if search:
        search_lower = search.lower()
        filtered_df = filtered_df[
            filtered_df["title"].str.lower().str.contains(search_lower) |
            filtered_df["organization"].str.lower().str.contains(search_lower)
        ]

    # Build a clean preview DataFrame with download links
    
    def make_table_link(row):
        return f'{CKAN_URL}/dataset/{row["name"]}'
    def make_download_link(row):
        return f'<a href="{CKAN_URL}/dataset/{row["name"]}" target="_blank">🔗 View Dataset</a>'

    filtered_df_display = filtered_df.copy()
    filtered_df_display["Dataset Link"] = filtered_df_display.apply(make_table_link, axis=1)
    view_mode = st.radio("📋 View mode", ["Table", "Detail (Markdown)"], horizontal=True)

    if view_mode == "Table":
        st.write(f"🔎 Showing **{len(filtered_df_display)}** dataset(s)")

        st.dataframe(
            filtered_df_display[["title", "organization", "resources", "last_modified", "views", "Dataset Link"]],
            use_container_width=True
        )
    
    elif view_mode == "Detail (Markdown)":
        st.write(f"🔎 Showing **{len(filtered_df_display)}** dataset(s)")
        for _, row in filtered_df_display.iterrows():
            st.markdown(f"""
            #### 📦 {row['title']}
            - 🏢 Org: {row['organization']}
            - 🗂️ Resources: {row['resources']}
            - 🕒 Last modified: {row['last_modified']}
            - 👁️ Views: {row['views']}
            - 🔗 Dataset: {make_download_link(row)}
            <hr style="margin:10px 0;">
            """, unsafe_allow_html=True)
    #st.dataframe(
    #    filtered_df_display[["title", "organization", "resources", "last_modified", "views", "Download Links"]],
    #    use_container_width=True
    #    )