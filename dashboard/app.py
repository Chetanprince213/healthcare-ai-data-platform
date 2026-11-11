
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "gold" / "healthcare_analytics.db"

st.set_page_config(
    page_title="Healthcare Analytics Platform",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Healthcare Analytics Platform")
st.caption(
    "Synthetic healthcare data | Bronze → Silver → Gold"
)

if not DB_PATH.exists():
    st.error(
        "Gold database not found. "
        "Run the Gold transformation first."
    )
    st.stop()


@st.cache_data
def load_data():
    with sqlite3.connect(DB_PATH) as connection:
        patients = pd.read_sql_query(
            "SELECT * FROM patients",
            connection
        )

        appointments = pd.read_sql_query(
            "SELECT * FROM appointments",
            connection
        )

    return patients, appointments


if st.button("Refresh data"):
    load_data.clear()

patients, appointments = load_data()

st.sidebar.header("Dashboard Filters")

departments = sorted(
    appointments["department"].dropna().unique().tolist()
)

selected_departments = st.sidebar.multiselect(
    "Select departments",
    options=departments,
    default=departments
)

filtered = appointments[
    appointments["department"].isin(selected_departments)
].copy()

filtered["appointment_date"] = pd.to_datetime(
    filtered["appointment_date"],
    errors="coerce"
)

total_patients = len(patients)
total_appointments = len(filtered)

completed = (
    filtered["status"] == "Completed"
).sum()

no_shows = (
    filtered["status"] == "No-Show"
).sum()

no_show_rate = (
    no_shows / total_appointments * 100
    if total_appointments > 0 else 0
)

st.subheader("Hospital KPIs")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Registered Patients", f"{total_patients:,}")
col2.metric("Appointments", f"{total_appointments:,}")
col3.metric("Completed", f"{completed:,}")
col4.metric("No-Show Rate", f"{no_show_rate:.2f}%")

st.divider()

st.subheader("Department Performance")

department_stats = (
    filtered.groupby("department")
    .agg(
        total_appointments=("appointment_id", "count"),
        no_shows=(
            "status",
            lambda values: (values == "No-Show").sum()
        )
    )
    .reset_index()
)

department_stats["no_show_rate_pct"] = (
    department_stats["no_shows"]
    / department_stats["total_appointments"]
    * 100
).round(2)

left, right = st.columns(2)

with left:
    st.write("Appointments by Department")
    st.bar_chart(
        department_stats.set_index("department")[
            "total_appointments"
        ]
    )

with right:
    st.write("No-Show Rate by Department (%)")
    st.bar_chart(
        department_stats.set_index("department")[
            "no_show_rate_pct"
        ]
    )

st.divider()

st.subheader("Monthly Appointment Trends")

monthly = filtered.dropna(
    subset=["appointment_date"]
).copy()

monthly["month"] = (
    monthly["appointment_date"]
    .dt.to_period("M")
    .astype(str)
)

monthly_stats = (
    monthly.groupby("month")
    .size()
    .reset_index(name="appointments")
    .sort_values("month")
)

st.line_chart(
    monthly_stats.set_index("month")["appointments"]
)

st.divider()

st.subheader("Appointment Status Distribution")

status_stats = (
    filtered["status"]
    .value_counts()
    .rename_axis("status")
    .reset_index(name="count")
)

st.bar_chart(
    status_stats.set_index("status")["count"]
)

st.divider()

st.subheader("Patient Demographics")

patients["age"] = pd.to_numeric(
    patients["age"],
    errors="coerce"
)

patients["age_group"] = pd.cut(
    patients["age"],
    bins=[0, 29, 44, 59, 120],
    labels=["0-29", "30-44", "45-59", "60+"]
)

age_stats = (
    patients.groupby(
        "age_group",
        observed=True
    )
    .size()
)

st.bar_chart(age_stats)

st.divider()

st.subheader("Appointment Records")

st.dataframe(
    filtered[
        [
            "appointment_id",
            "patient_id",
            "appointment_date",
            "department",
            "status"
        ]
    ],
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Portfolio demonstration using synthetic data only. "
    "Not for clinical decision-making."
)
