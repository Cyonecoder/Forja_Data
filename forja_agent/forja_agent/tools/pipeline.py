from langchain_core.tools import tool

@tool
def get_pipeline_status(sections: list[str] | None = None) -> dict:
    """
    Returns the Forja pipeline health report grouped by section.
    sections can contain: containers, kafka, minio, postgres, logs.
    """
    selected = sections or ["containers", "kafka", "minio", "postgres", "logs"]
    data = {
        "containers": {
            "forjakafka": "running",
            "forjapostgres": "running",
            "forjaminio": "running",
        },
        "kafka": {
            "ga4.events": {"lag": 0, "messages": 123456},
        },
        "minio": {
            "bronze_files_today": 42,
        },
        "postgres": {
            "golddailyusersv2_rows": 120034,
        },
        "logs": {
            "recent_errors": 0,
        },
    }
    return {k: v for k, v in data.items() if k in selected}