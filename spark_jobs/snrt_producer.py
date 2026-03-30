import os, psycopg2, json, logging, time
from kafka import KafkaProducer
from datetime import datetime, date
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-PRODUCER] %(levelname)s — %(message)s")
log = logging.getLogger("SNRT-PRODUCER")

PG_HOST     = os.getenv("POSTGRES_HOST",     "169.255.179.24")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB",       "snrt_stats")
PG_USER     = os.getenv("POSTGRES_USER",     "snrt_readonly")
PG_PASS     = os.getenv("POSTGRES_PASSWORD", "6AOd3Dm2")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INTERVAL    = 300  # toutes les 5 min

# Tables → topics Kafka
TABLES = {
    "watchings":           "snrt-watchings",
    "users":               "snrt-users",
    "profiles":            "snrt-profiles",
    "contents":            "snrt-contents",
    "subscriptions":       "snrt-subscriptions",
    "user_subscriptions":  "snrt-user-subscriptions",
    "programs":            "snrt-programs",
    "category_category":   "snrt-categories",
    "user_fav":            "snrt-user-fav",
    "user_like":           "snrt-user-like",
}

# Colonnes timestamp par table pour le delta live (watermark)
TIMESTAMP_COLS = {
    "watchings":          "created_at",
    "users":              "created_at",
    "profiles":           "created_at",
    "user_subscriptions": "created_at",
    "user_fav":           "created_at",
    "user_like":          "created_at",
}

def serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} non sérialisable")

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=serialize).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=10,
        batch_size=32768,
        retries=3
    )

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
        connect_timeout=10
    )

# Watermarks en mémoire (reset au restart — suffisant pour du live)
watermarks = {}

def pump_table(cur, producer, table, topic):
    ts_col = TIMESTAMP_COLS.get(table)
    last_ts = watermarks.get(table)

    if ts_col and last_ts:
        query = f"SELECT * FROM {table} WHERE {ts_col} > %s ORDER BY {ts_col} LIMIT 100000;"
        cur.execute(query, (last_ts,))
    else:
        query = f"SELECT * FROM {table} LIMIT 100000;"
        cur.execute(query)

    cols  = [d[0] for d in cur.description]
    rows  = cur.fetchall()
    count = 0
    new_ts = last_ts

    for row in rows:
        record = dict(zip(cols, row))
        record["_ingested_at"] = datetime.utcnow().isoformat()
        key = record.get("id", count)
        producer.send(topic, key=key, value=record)
        count += 1
        # Met à jour le watermark
        if ts_col and record.get(ts_col):
            val = record[ts_col]
            if new_ts is None or val > new_ts:
                new_ts = val

    producer.flush()
    if count > 0:
        log.info(f"  ✅ {table} → {topic} : {count} lignes envoyées")
        if ts_col and new_ts:
            watermarks[table] = new_ts
            log.info(f"     💧 Watermark {table}: {new_ts}")
    else:
        log.info(f"  ⏳ {table} : aucune nouvelle ligne")

def main():
    log.info("🚀 SNRT Producer démarré — live streaming toutes les 5 min")
    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'='*50}")
        log.info(f"🔄 Cycle #{cycle} — {datetime.utcnow()} UTC")
        try:
            producer = get_producer()
            conn     = get_conn()
            conn.autocommit = True
            cur      = conn.cursor()
            for table, topic in TABLES.items():
                try:
                    pump_table(cur, producer, table, topic)
                except Exception as e:
                    log.error(f"  ❌ {table}: {e}")
            cur.close()
            conn.close()
            producer.close()
            log.info(f"✅ Cycle #{cycle} terminé")
        except Exception as e:
            log.error(f"❌ Erreur connexion: {e}")
        log.info(f"⏳ Prochain cycle dans {INTERVAL}s...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
