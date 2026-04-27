import pytest
import psycopg2
import subprocess
from datetime import datetime, timedelta

DB_PARAMS = {
    "host": "172.21.0.11",
    "port": 5432,
    "dbname": "snrt_stats",
    "user": "snrt_readonly",
    "password": "6AOd3Dm2"
}

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

def test_gold_snrt_not_empty():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_snrt_content_performance")
    assert cur.fetchone()[0] > 0
    conn.close()

def test_gold_ga4_not_empty():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_ga4_daily_stats")
    assert cur.fetchone()[0] > 0
    conn.close()

def test_gold_snrt_freshness_j1():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(updated_at) FROM gold_snrt_content_performance")
    last = cur.fetchone()[0]
    assert last > datetime.now() - timedelta(days=1), f"Gold SNRT pas à jour J-1! Dernière MAJ: {last}"
    conn.close()

def test_gold_ga4_freshness_j1():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(report_date) FROM gold_ga4_daily_stats")
    last = cur.fetchone()[0]
    assert last >= (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"), f"Gold GA4 pas à jour J-1! Dernière date: {last}"
    conn.close()

def test_dim_time_exists():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_time")
    assert cur.fetchone()[0] > 1000
    conn.close()

def test_kpis_columns_exist():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'gold_snrt_content_performance'
        AND column_name IN ('watch_time_total', 'completion_rate')
    """)
    cols = [r[0] for r in cur.fetchall()]
    assert len(cols) == 2, f"Colonnes KPIs manquantes: {cols}"
    conn.close()

def test_kpis_not_zero():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_snrt_content_performance WHERE watch_time_total > 0")
    assert cur.fetchone()[0] > 0, "watch_time_total est à 0 partout!"
    conn.close()

def test_kafka_topics():
    result = subprocess.run(
        ["docker", "exec", "forja_kafka",
         "bash", "-c",
         "kafka-topics --bootstrap-server localhost:9092 --list"],
        capture_output=True, text=True
    )
    assert "snrt-contents" in result.stdout, f"Topics Kafka manquants: {result.stdout}{result.stderr}"

def test_gold_engagement_not_empty():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_snrt_engagement")
    assert cur.fetchone()[0] > 0
    conn.close()
