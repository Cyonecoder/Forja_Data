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
FORJA_CONTAINER_WHITELIST = [
    "forjazookeeper",
    "forjakafka",
    "forjaschemaregistry",
    "forjakafkaui",
    "forjapostgres",
    "forjaminio",
    "forjasparkmaster",
    "forjasparkworker",
    "forjasparkworkersilver",
    "forjabronzeconsumer",
    "ga4-producer",
    "forjaairflowwebserver",
    "forjaairflowscheduler",
    "forjagrafana",
    "forja-agent",
    "ollama",
    "vllm",
]

# ========== KAFKA TOPIC WHITELIST ==========
FORJA_KAFKA_TOPIC_WHITELIST = [
    "ga4.events",
    "ga4.raw",
    "snrt.watchings",
    "bronze.ga4",
    "bronze.snrt",
    "silver.sessions",
    "silver.users",
    "silver.programs",
]

# ========== GOLD TABLE WHITELIST ==========
# Tables Gold autorisées pour les requêtes
FORJA_GOLD_TABLE_WHITELIST = [
    "gold_daily_users_v2",
    "gold_weekly_retention",
    "gold_monthly_active_users",
    "gold_content_performance",
    "gold_user_segments",
    "gold_realtime_viewers",
    "gold_program_ratings",
    "gold_ad_inventory",
]

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