import streamlit as st
import requests
import pandas as pd
import altair as alt
import os
from collections import defaultdict

# === CONFIG ===
CKAN_URL = "https://gdcatalognhic.nha.co.th"  # no trailing slash
API_KEY = os.getenv("CKAN_API_KEY")
headers = {"Authorization": API_KEY}

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

@st.cache_data(ttl=600)
def get_dataset_detail(dataset_id):
    r = requests.get(f"{CKAN_URL}/api/3/action/package_show?id={dataset_id}", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return None

@st.cache_data(ttl=600)
def get_org_detail(org_id):
    r = requests.get(f"{CKAN_URL}/api/3/action/organization_show?id={org_id}", headers=headers)
    if r.status_code == 200:
        return r.json()["result"]
    return None

@st.cache_data(ttl=600)
def search_datasets(limit=10000):
    url = f"{CKAN_URL}/api/3/action/package_search"
    params = {"rows": limit}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code == 200:
        return r.json()["result"]["results"]
    return []


# === DATA LOADING ===
datasets = get_datasets()
orgs = get_organizations()

# Build dataset details + index
dataset_data = search_datasets()
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

df_datasets = pd.DataFrame([{
    "title": d["title"],
    "name": d["name"],
    "organization": d["organization"]["title"] if d.get("organization") else "—",
    "resources": len(d["resources"]),
    "last_modified": d.get("metadata_modified", "—"),
    "views": d.get("tracking_summary", {}).get("total", 0)
} for d in dataset_data])

# === TABS ===
tab1, tab2 = st.tabs(["📊 Overview", "🔍 Explorer"])

# === TAB 1: OVERVIEW ===
with tab1:
    col1, col2, col3 = st.columns(3)
    col1.metric("🏢 Organizations", len(orgs))
    col2.metric("📦 Datasets", len(datasets))
    col3.metric("👁️ Website Views (tracked)", total_views)

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
    org_titles = df_datasets["organization"].dropna().unique().tolist()
    org_filter = st.selectbox("🏢 Filter by organization", ["All"] + sorted(org_titles))    

    filtered_df = df_datasets.copy()

    if org_filter != "All":
        filtered_df = filtered_df[filtered_df["organization"] == org_filter]

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

    #def make_download_links(row):
    #    for pkg in dataset_data:
    #        if pkg.get("name") == row["name"]:
    #            if pkg.get("resources"):
    #                links = [
    #                    f'<a href="{r["url"]}" target="_blank">{r["name"]}</a>'
    #                    for r in pkg["resources"]
    #                ]
    #                return "<br>".join(links)
    #            break
    #    return "—"

    filtered_df_display = filtered_df.copy()
    #filtered_df_display["Download Links"] = filtered_df_display.apply(make_download_links, axis=1)
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