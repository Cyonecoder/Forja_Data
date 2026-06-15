from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # LLM Supervisor (celui qui supervise les décisions)
    supervisor_llm_url: str = "http://localhost:11434/v1"
    supervisor_llm_model: str = "qwen3:14b"

    # LLM Judge (celui qui fait les guardrails)
    judge_llm_url: str = "http://localhost:11434/v1"
    judge_llm_model: str = "qwen3:14b"

    # Pour switcher en prod: changer les URLs vers vLLM
    # Exemple prod: supervisor_llm_url = "http://vllm:8000/v1"

    # App config
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "dev"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()