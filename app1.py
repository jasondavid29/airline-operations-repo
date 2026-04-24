import streamlit as st
import snowflake.connector
import pandas as pd

# -----------------------------
# CONNECTION (SECURE)
# -----------------------------
conn = snowflake.connector.connect(
    user='pranee',
    password='Mahi1234567890',
    account='MYYCKPD-SI60270',
    warehouse='COMPUTE_WH',
    database='AIR',
    schema='CURATED'
)

def run_query(query):
    return pd.read_sql(query, conn)

st.title("✈️ Airline Operations Dashboard (Enterprise + AI)")

# =========================================================
# KPI SECTION
# =========================================================
st.header("📊 KPI Overview")

kpi = run_query("SELECT * FROM KPI_ON_TIME_PERFORMANCE")

avg_delay = run_query("""
SELECT AVG(delay_minutes) AS avg_delay
FROM FACT_DISRUPTION_EVENT
""")

impacted = run_query("""
SELECT COUNT(DISTINCT flight_key) AS impacted_flights
FROM FACT_DISRUPTION_EVENT
""")

# NEW KPIs (Task Requirement)
readiness = run_query("""
SELECT COUNT(*) AS breaches
FROM STG.STG_AIRPORT_TURNAROUND_LOGS
WHERE readiness_status = 'AT_RISK'
""")

severity = run_query("""
SELECT severity, COUNT(*) AS cnt
FROM STG.STG_DISRUPTION_ALERTS
GROUP BY severity
""")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Flights", int(kpi['TOTAL_FLIGHTS'][0]))
col2.metric("Delayed Flights", int(kpi['DELAYED_FLIGHTS'][0]))
col3.metric("Avg Delay", round(avg_delay['AVG_DELAY'][0], 2))
col4.metric("Readiness Breaches", int(readiness['BREACHES'][0]))

st.metric("Impacted Flights", int(impacted['IMPACTED_FLIGHTS'][0]))

st.subheader("Disruptions by Severity")
st.bar_chart(severity.set_index('SEVERITY'))

# =========================================================
# TREND ANALYSIS
# =========================================================
st.header("📈 Flight Trend")

trend = run_query("SELECT * FROM KPI_FLIGHT_TREND")
st.line_chart(trend.set_index('FLIGHT_DATE'))

# ROUTE DELAY TREND (Task)
route_trend = run_query("""
SELECT
    df.origin_airport || '-' || df.destination_airport AS route,
    AVG(e.delay_minutes) AS avg_delay
FROM FACT_DISRUPTION_EVENT e
JOIN DIM_FLIGHT df ON e.flight_key = df.flight_key
GROUP BY route
ORDER BY avg_delay DESC
LIMIT 10
""")

st.subheader("Route Delay Trend")
st.bar_chart(route_trend.set_index('ROUTE'))

# AIRPORT DISRUPTION TREND (Task)
airport_trend = run_query("""
SELECT origin_airport, COUNT(*) AS disruptions
FROM FACT_DISRUPTION_EVENT e
JOIN DIM_FLIGHT df ON e.flight_key = df.flight_key
GROUP BY origin_airport
""")

st.subheader("Airport Disruption Trend")
st.bar_chart(airport_trend.set_index('ORIGIN_AIRPORT'))

# =========================================================
# EXISTING AIRPORT DELAY
# =========================================================
st.header("🏢 Airport Delay")

airport = run_query("SELECT * FROM KPI_AIRPORT_DELAY")
st.bar_chart(airport.set_index('ORIGIN_AIRPORT'))

# =========================================================
# CREW UTILIZATION
# =========================================================
st.header("👨‍✈️ Crew Utilization")

crew = run_query("SELECT * FROM KPI_CREW_UTILIZATION LIMIT 20")
st.bar_chart(crew.set_index('CREW_ID'))

# =========================================================
# MAINTENANCE KPI
# =========================================================
st.header("🛠️ Maintenance Readiness")

maint = run_query("SELECT * FROM KPI_MAINTENANCE")
st.dataframe(maint)

# =========================================================
# COMPARISON VIEWS (Task)
# =========================================================
st.header("📊 Comparison Analysis")

# Aircraft vs Avg Delay
aircraft_delay = run_query("""
SELECT
    da.aircraft_type,
    AVG(e.delay_minutes) AS avg_delay
FROM FACT_DISRUPTION_EVENT e
JOIN FACT_FLIGHT_OPERATION f ON e.flight_key = f.flight_key
JOIN DIM_AIRCRAFT da ON f.aircraft_key = da.aircraft_key
GROUP BY da.aircraft_type
""")

st.subheader("Aircraft Type vs Avg Delay")
st.bar_chart(aircraft_delay.set_index('AIRCRAFT_TYPE'))

# Airport vs Turnaround Efficiency
turnaround = run_query("""
SELECT
    airport_code,
    COUNT(*) AS total,
    SUM(CASE WHEN readiness_status='READY' THEN 1 ELSE 0 END) AS ready
FROM STG.STG_AIRPORT_TURNAROUND_LOGS
GROUP BY airport_code
""")

st.subheader("Airport vs Turnaround Efficiency")
st.bar_chart(turnaround.set_index('AIRPORT_CODE'))

# =========================================================
# DRILL DOWN
# =========================================================
st.header("🔍 Drill Down Analysis")

# High Risk Flights
high_delay = run_query("""
SELECT
    df.flight_id,
    e.delay_minutes,
    e.event_type
FROM FACT_DISRUPTION_EVENT e
JOIN DIM_FLIGHT df ON e.flight_key = df.flight_key
WHERE e.delay_minutes > 30
LIMIT 20
""")

st.subheader("High Risk Flights")
st.dataframe(high_delay)

# Repeated Disruption Routes
repeat_routes = run_query("""
SELECT
    df.origin_airport,
    df.destination_airport,
    COUNT(*) AS disruptions
FROM FACT_DISRUPTION_EVENT e
JOIN DIM_FLIGHT df ON e.flight_key = df.flight_key
GROUP BY df.origin_airport, df.destination_airport
HAVING COUNT(*) > 2
""")

st.subheader("Repeated Disruption Routes")
st.dataframe(repeat_routes)

# Aircraft Readiness Issues
aircraft_issues = run_query("""
SELECT
    da.aircraft_id,
    COUNT(*) AS issues
FROM FACT_MAINTENANCE_CLEARANCE f
JOIN DIM_AIRCRAFT da ON f.aircraft_key = da.aircraft_key
WHERE f.status != 'CLEARED'
GROUP BY da.aircraft_id
""")

st.subheader("Aircraft with Readiness Issues")
st.dataframe(aircraft_issues)

# =========================================================
# AI INSIGHTS (CORTEX)
# =========================================================
st.header("🤖 AI Insights")

ai = run_query("""
SELECT
    df.flight_id,
    SNOWFLAKE.CORTEX.SUMMARIZE(
        CONCAT(
            'Flight delay of ',
            e.delay_minutes,
            ' minutes due to ',
            e.event_type
        )
    ) AS summary
FROM FACT_DISRUPTION_EVENT e
JOIN DIM_FLIGHT df ON e.flight_key = df.flight_key
LIMIT 5
""")

for _, row in ai.iterrows():
    st.write(f"✈️ Flight {row['FLIGHT_ID']}: {row['SUMMARY']}")

# =========================================================
# SENTIMENT ANALYSIS
# =========================================================
st.header("😊 Sentiment Analysis")

sentiment = run_query("""
SELECT
    event_type,
    SNOWFLAKE.CORTEX.SENTIMENT(
        CONCAT('Delay caused by ', event_type)
    ) AS sentiment
FROM FACT_DISRUPTION_EVENT
LIMIT 10
""")

st.dataframe(sentiment)