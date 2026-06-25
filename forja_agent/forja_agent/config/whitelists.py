# Whitelists de sécurité pour le Forja Agent

# ========== LLM WHITELIST ==========
# URLs autorisées pour les appels LLM (empêche appels vers serveurs externes)
FORJA_LLM_URL_WHITELIST = [
    "http://localhost:11434",      # Ollama local
    "http://127.0.0.1:11434",
    "http://ollama:11434",          # Ollama dans Docker
    "http://host.docker.internal:11434",
    "http://localhost:8000",        # vLLM local
    "http://127.0.0.1:8000",
    "http://vllm:8000",             # vLLM dans Docker
]

# Modèles LLM autorisés
FORJA_LLM_MODEL_WHITELIST = [
    "qwen3:14b",
    "qwen3:32b",
    "llama3.2:3b",
    "llama3.2",
    "mistral",
]

# ========== DATABASE WHITELIST ==========
# Hosts de bases de données autorisés
FORJA_DB_HOST_WHITELIST = [
    "localhost",
    "127.0.0.1",
    "host.docker.internal",
    "forjapostgres",
    "postgres",
]

FORJA_DB_PORT_WHITELIST = [5432, 5433]
FORJA_DB_NAME_WHITELIST = ["forja", "forja_db"]

# ========== LANGFUSE CONFIG ==========
# Hôtes LangFuse autorisés pour le tracing
FORJA_LANGFUSE_HOST_WHITELIST = [
    "https://cloud.langfuse.com",
    "https://us.cloud.langfuse.com",
    "https://eu.cloud.langfuse.com",
]

# ========== AUTH TOKEN ==========
# Pattern pour valider le token d'authentification
FORJA_AUTH_TOKEN_PREFIX = "forja-"

# ========== KAFKA CONFIG ==========
# Bootstrap servers Kafka autorisés
FORJA_KAFKA_BOOTSTRAP_SERVERS_WHITELIST = [
    "localhost:9092",
    "127.0.0.1:9092",
    "host.docker.internal:9092",
    "forjakafka:9092",
    "kafka:9092",
    "forjakafka:29092",
]

# Protocoles de sécurité autorisés
FORJA_KAFKA_SECURITY_PROTOCOL_WHITELIST = [
    "PLAINTEXT",
    "SASL_SSL",
]

# ========== CONTAINER WHITELIST ==========
# EXACT container_name values from docker-compose.yml and
# docker-compose.airflow.yml. The pipeline health checker inspects ONLY these
# names and never enumerates arbitrary host containers.
FORJA_CONTAINER_WHITELIST = [
    # docker-compose.yml
    "forja_zookeeper",
    "forja_kafka",
    "forja_schema_registry",
    "forja_kafka_ui",
    "forja_postgres",
    "forja_minio",
    "forja_spark_master",
    "ga4-producer",
    "forja_spark_worker_silver",
    "forja_bronze_consumer",
    # docker-compose.airflow.yml
    "forja_airflow_webserver",
    "forja_airflow_scheduler",
]

# Containers whose failure takes the whole pipeline "down" (vs. "degraded").
# Kafka is the ingestion backbone and depends on Zookeeper. The gold/analytics
# Postgres is EXTERNAL (snrt_stats), so the local forja_postgres container is
# NOT marked critical here; Postgres health is covered by _check_postgres.
CRITICAL_CONTAINERS = {
    "forja_kafka",
    "forja_zookeeper",
}

# ========== KAFKA TOPIC WHITELIST ==========
# Real topics seeded from root .env.example (KAFKA_TOPIC_GA4 / KAFKA_TOPIC_SNRT)
# and the SNRT producers. "snrt-contents" is confirmed real (snrt_producer.py
# + root tests/test_pipeline.py::test_kafka_topics). The health checker inspects
# ONLY these topics.
KAFKA_TOPIC_WHITELIST = [
    "ga4.events",
    "snrt.actions",
    "snrt-contents",
]

# ========== GOLD TABLE WHITELIST ==========
# Real gold tables exercised by the pipeline tests (root tests/test_pipeline.py).
# Freshness queries use ONLY these identifiers, never user-supplied names.
GOLD_TABLE_WHITELIST = [
    "gold_snrt_content_performance",
    "gold_ga4_daily_stats",
    "gold_snrt_engagement",
    "dim_time",
]

# ----- Backwards-compat aliases (older WP code referenced FORJA_* names) -----
FORJA_KAFKA_TOPIC_WHITELIST = KAFKA_TOPIC_WHITELIST
FORJA_GOLD_TABLE_WHITELIST = GOLD_TABLE_WHITELIST

# ========== SILVER TABLE WHITELIST ==========
# Tables Silver autorisées pour les requêtes
FORJA_SILVER_TABLE_WHITELIST = [
    "silver_sessions",
    "silver_users",
    "silver_programs",
    "silver_events_enriched",
    "silver_content_metadata",
    "silver_user_behavior",
]

# ========== ALL ALLOWED TABLES ==========
FORJA_ALLOWED_TABLES = FORJA_GOLD_TABLE_WHITELIST + FORJA_SILVER_TABLE_WHITELIST