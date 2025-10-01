# FILE: app.py
import importlib
from io import BytesIO

import pandas as pd
import streamlit as st

import brixtonjobs  # our scraper lib

# If you hot-edit the library, reload on each run:
importlib.reload(brixtonjobs)

st.set_page_config(page_title="Brixton Job Scraper", layout="wide")
st.title("Brixton Job Scraper")

# Show which file is actually imported (to avoid confusion)
st.caption(f"Using scraper module: {brixtonjobs.__file__}")

# One button with a stable, unique key (prevents duplicate-ID errors)
if st.button("Fetch Jobs", key="fetch_jobs_btn"):
    try:
        with st.spinner("Fetching jobs and descriptions…"):
            # JSON API first; if it fails, code falls back to HTML automatically
            df = brixtonjobs.fetch_jobs(mode="auto", max_pages=None, page_size=50, fetch_details=True)

        if df.empty:
            st.warning("No jobs found.")
        else:
            # Tell you which path was used
            if brixtonjobs.used_fallback:
                st.info("HTML fallback was used (JSON API failed).")

            # Make nice preview columns & order
            if "description" in df.columns:
                if "desc_snippet" not in df.columns:
                    df["desc_snippet"] = df["description"].astype(str).str.slice(0, 220)

            preferred = [c for c in ["title","position_type","work_model","city","state","detail_href","desc_snippet"] if c in df.columns]
            rest = [c for c in df.columns if c not in preferred]
            df = df[preferred + rest]

            st.success(f"Fetched {len(df)} jobs")
            st.dataframe(df, use_container_width=True)

            # Excel download (openpyxl)
            output = BytesIO()
            df.to_excel(output, index=False, engine="openpyxl")
            st.download_button(
                label="Download as Excel",
                data=output.getvalue(),
                file_name="brixton_jobs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_btn",
            )

    except Exception as e:
        st.error(f"Error fetching jobs: {e}")