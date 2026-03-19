import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection
[snowflake]
user = "RajyaLakshmi"
password = "Raji@35210382004"
account = "WXHNFVF-FL27943"
warehouse = "cityride_wh"
database = "cityride"
schema = "curated"


# Function to run query
def run_query(query):
    return pd.read_sql(query, conn)

st.title("🚴 CityRide Analytics Dashboard")

# KPI 1: Anomalous Rental Probability
kpi1 = run_query("""
SELECT (COUNT_IF(is_flagged=1) * 100.0 / COUNT(*)) AS value
FROM fact_rentals
""")
st.metric("Anomalous Rental %", round(kpi1['VALUE'][0],2))

# KPI 3: Engagement Ratio
kpi3 = run_query("""
SELECT 
(COUNT(DISTINCT user_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM dim_users),0)) 
AS value
FROM fact_rentals
WHERE start_time >= DATEADD(DAY,-30,CURRENT_DATE)
""")
st.metric("Engagement %", round(kpi3['VALUE'][0],2))

# KPI 4: Fleet Health
kpi4 = run_query("""
SELECT (COUNT_IF(battery_level >= 25) * 100.0 / COUNT(*)) AS value
FROM dim_bikes
""")
st.metric("Fleet Health %", round(kpi4['VALUE'][0],2))

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

st.subheader("Station Availability Ratio")
st.bar_chart(df_station.set_index('STATION_NAME'))

# KPI 5: Revenue by Channel
df_rev = run_query("""
SELECT channel, AVG(price) AS avg_revenue
FROM fact_rentals
GROUP BY channel
""")

st.subheader("Revenue by Channel")
st.bar_chart(df_rev.set_index('CHANNEL'))
