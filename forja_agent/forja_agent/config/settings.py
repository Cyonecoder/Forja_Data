from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ---- LLM (LOCAL ONLY: Ollama/vLLM OpenAI-compatible endpoints) ----
    # api_key is a dummy value required only by the OpenAI-compatible client
    # interface. No cloud API is ever used.
    supervisor_llm_url: str = "http://localhost:11434/v1"
    supervisor_llm_model: str = "qwen3:14b"
    supervisor_llm_api_key: str = "ollama"
    supervisor_llm_api_key2: str = "gpt-4.0"

    judge_llm_url: str = "http://localhost:11434/v1"
    judge_llm_model: str = "qwen3:14b"
    judge_llm_api_key: str = "ollama"

    llm_provider: str = "ollama"

    # Pour switcher en prod: changer les URLs vers vLLM
    # Exemple prod: supervisor_llm_url = "http://vllm:8000/v1"

    # ---- App config ----
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "dev"

    # ---- Langfuse (optional, local-only by default / disabled) ----
    langfuse_host: str | None = None
    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None

    # ---- Auth ----
    auth_token: str = "dev-token"

    # ---- Kafka ----
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_topic_events: str = "ga4.events"

    # ---- Gold / analytics Postgres (READ-ONLY health checks) ----
    # NOTE: the gold tables live in the external snrt_stats DB used by the
    # pipeline tests (see root tests/test_pipeline.py), not necessarily a local
    # container. Override pg_host/pg_password via .env to point at the real DB;
    # defaults are safe placeholders so settings load without secrets.
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "snrt_stats"
    pg_user: str = "snrt_readonly"
    pg_password: str = ""  # from .env, never hardcode a real secret
    pg_connect_timeout: int = 5

    # ---- Database (legacy local forja DB, unrelated to gold checks) ----
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "forja"
    db_user: str = "forja_user"
    db_password: str = "forja_password"

    database_url: str | None = None
    agent_database_url: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()


def get_settings() -> Settings:
    return settings
