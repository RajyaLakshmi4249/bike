import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection using secrets
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse=st.secrets["snowflake"]["warehouse"],
    database=st.secrets["snowflake"]["database"],
    schema=st.secrets["snowflake"]["schema"]
)

# Function to run query
def run_query(query):
    return pd.read_sql(query, conn)

st.title("🚴 CityRide Analytics Dashboard")

# KPI 1: Anomalous Rental Probability
kpi1 = run_query("""
SELECT (COUNT_IF(is_flagged=1) * 100.0 / COUNT(*)) AS value
FROM fact_rentals
""")
if not kpi1.empty and pd.notnull(kpi1.iloc[0,0]):
    st.metric("Anomalous Rental %", round(float(kpi1.iloc[0,0]), 2))
else:
    st.metric("Anomalous Rental %", "No data")

# KPI 3: Engagement Ratio
kpi3 = run_query("""
SELECT 
(COUNT(DISTINCT user_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM dim_users),0)) 
AS value
FROM fact_rentals
WHERE start_time >= DATEADD(DAY,-30,CURRENT_DATE)
""")
if not kpi3.empty and pd.notnull(kpi3.iloc[0,0]):
    st.metric("Engagement %", round(float(kpi3.iloc[0,0]), 2))
else:
    st.metric("Engagement %", "No data")

# KPI 4: Fleet Health
kpi4 = run_query("""
SELECT (COUNT_IF(battery_level >= 25) * 100.0 / COUNT(*)) AS value
FROM dim_bikes
""")
if not kpi4.empty and pd.notnull(kpi4.iloc[0,0]):
    st.metric("Fleet Health %", round(float(kpi4.iloc[0,0]), 2))
else:
    st.metric("Fleet Health %", "No data")

# KPI 2: Station Availability Chart
df_station = run_query("""
SELECT 
  s.station_name,
  ((s.capacity - COUNT(r.bike_id)) * 100.0 / NULLIF(s.capacity,0)) AS availability_ratio
FROM dim_stations s
LEFT JOIN fact_rentals r 
  ON s.station_id = r.end_station_id
GROUP BY s.station_name, s.capacity
""")
if not df_station.empty:
    st.subheader("Station Availability Ratio")
    st.bar_chart(df_station.set_index('STATION_NAME'))
else:
    st.subheader("Station Availability Ratio")
    st.write("No data available")

# KPI 5: Revenue by Channel
df_rev = run_query("""
SELECT channel, AVG(price) AS avg_revenue
FROM fact_rentals
GROUP BY channel
""")
if not df_rev.empty:
    st.subheader("Revenue by Channel")
    st.bar_chart(df_rev.set_index('CHANNEL'))
else:
    st.subheader("Revenue by Channel")
    st.write("No data available")
