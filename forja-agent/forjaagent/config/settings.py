from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    supervisor_llm_url: str = "http://localhost:11434/v1"
    supervisor_llm_model: str = "qwen3:14b"
    judge_llm_url: str = "http://localhost:11434/v1"
    judge_llm_model: str = "qwen3:14b"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "dev"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()