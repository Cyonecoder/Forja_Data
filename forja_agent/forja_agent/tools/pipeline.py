"""WP2: read-only Forja pipeline health checker.

``get_pipeline_status`` inspects ONLY whitelisted Docker containers, Kafka
topics and gold tables and reports a structured, JSON-serializable health
report. It is strictly READ-ONLY:

* never restarts / mutates / removes containers,
* never runs DDL/DML or arbitrary SQL (only ``SELECT 1``, ``COUNT(*)`` and
  ``MIN/MAX`` over whitelisted identifiers),
* never publishes to Kafka,
* never touches a name that is not in a whitelist constant.

Every section is isolated: a failure in one section degrades that section only
and never aborts the others or raises out of the tool.

Heavy clients (docker / psycopg / kafka) are imported lazily at module scope so
that importing this module never requires those libraries to be installed, and
tests can patch the module-level references without a real backend.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Callable, Literal

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from forja_agent.config.settings import get_settings
from forja_agent.config.whitelists import (
    CRITICAL_CONTAINERS,
    FORJA_CONTAINER_WHITELIST,
    GOLD_TABLE_WHITELIST,
    KAFKA_TOPIC_WHITELIST,
)

# --- Lazy/optional backend libraries (patchable in tests) --------------------
try:  # pragma: no cover - import guard
    import docker  # type: ignore
except Exception:  # pragma: no cover
    docker = None  # type: ignore

try:  # pragma: no cover - import guard
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:  # pragma: no cover - import guard
    import kafka  # type: ignore
except Exception:  # pragma: no cover
    kafka = None  # type: ignore


# --- Constants ---------------------------------------------------------------
PipelineSection = Literal["containers", "postgres", "gold_tables", "kafka", "logs"]
ALL_SECTIONS: list[PipelineSection] = [
    "containers",
    "postgres",
    "gold_tables",
    "kafka",
    "logs",
]

# Candidate freshness columns, tried in this order.
DATE_COLUMN_CANDIDATES = [
    "report_date",
    "date",
    "day",
    "event_date",
    "created_at",
    "updated_at",
]

# Log lines containing any of these (case-insensitive) are flagged as errors.
LOG_ERROR_PATTERNS = [
    "ERROR",
    "Exception",
    "Traceback",
    "FAILED",
    "timeout",
    "connection refused",
    "FATAL",
]

LOG_TAIL = 50          # default tail
LOG_TAIL_MAX = 100     # hard cap
EXCERPT_MAX = 2000     # truncate excerpts to this many chars


# --- Input schema ------------------------------------------------------------
class GetPipelineStatusInput(BaseModel):
    sections: list[PipelineSection] | None = Field(
        default=None,
        description="Sections to check; if omitted, all are checked.",
    )


# --- Helpers -----------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_section(name: str, fn: Callable[[], dict]) -> dict:
    """Run a section check; never let it raise out of the tool."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - last-resort isolation
        return {"status": "degraded", "errors": [str(exc)]}


def _section_defaults(name: str) -> dict:
    """Full required sub-shape for a section (used for skipped/fallback)."""
    base = {
        "containers": {"status": "skipped", "items": [], "errors": []},
        "postgres": {
            "status": "skipped",
            "connection": "skipped",
            "latency_ms": None,
            "errors": [],
        },
        "gold_tables": {"status": "skipped", "tables": [], "errors": []},
        "kafka": {"status": "skipped", "topics": [], "errors": []},
        "logs": {"status": "skipped", "recent_errors": [], "errors": []},
    }
    return base[name]


def redact_secrets(text: str) -> str:
    """Mask secrets in log/excerpt text. Never return raw credentials."""
    if not text:
        return text
    # KEY=VALUE style secrets (PASSWORD=, SECRET=, TOKEN=, KEY= and *_KEY=).
    text = re.sub(
        r"(?i)((?:PASSWORD|SECRET|TOKEN|[A-Z0-9_]*KEY)\s*[=:]\s*)(\S+)",
        r"\1***REDACTED***",
        text,
    )
    # Credentials embedded in DB / broker URLs (scheme://user:pass@host).
    text = re.sub(
        r"([a-zA-Z][a-zA-Z0-9+.\-]*://)([^@\s/]+)@",
        r"\1***REDACTED***@",
        text,
    )
    # JWT-like strings (header.payload.signature).
    text = re.sub(
        r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+",
        "***REDACTED_JWT***",
        text,
    )
    return text


def _truncate(text: str, limit: int = EXCERPT_MAX) -> str:
    if text is None:
        return ""
    return text if len(text) <= limit else text[:limit]


def _pg_connect(settings):
    """Open a read-only-style psycopg connection from settings.

    Raises if psycopg is unavailable or the connection fails. Callers translate
    failures into safe section errors (never echoing the password/DSN).
    """
    if psycopg is None:
        raise RuntimeError("psycopg is not installed")
    return psycopg.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_db,
        user=settings.pg_user,
        password=settings.pg_password,
        connect_timeout=settings.pg_connect_timeout,
    )


# --- Section: containers -----------------------------------------------------
def _check_containers() -> dict:
    errors: list[str] = []
    items: list[dict] = []

    if docker is None:
        return {
            "status": "degraded",
            "items": [],
            "errors": ["docker SDK not available"],
        }

    try:
        client = docker.from_env()
    except Exception as exc:  # noqa: BLE001 - socket unavailable
        return {
            "status": "degraded",
            "items": [],
            "errors": [f"docker socket unavailable: {exc}"],
        }

    critical_down = False
    noncritical_issue = False

    # Inspect ONLY whitelisted names; never list all host containers.
    for name in FORJA_CONTAINER_WHITELIST:
        is_critical = name in CRITICAL_CONTAINERS
        try:
            container = client.containers.get(name)
            attrs = getattr(container, "attrs", {}) or {}
            state = attrs.get("State", {}) or {}
            status = getattr(container, "status", None) or state.get("Status", "unknown")
            image_tags = getattr(getattr(container, "image", None), "tags", None) or []
            image = image_tags[0] if image_tags else attrs.get("Config", {}).get("Image", "")
            health = (state.get("Health") or {}).get("Status")
            running = status == "running"
            healthy = running and (health in (None, "healthy"))
            items.append(
                {
                    "name": name,
                    "found": True,
                    "status": status,
                    "image": image,
                    "started_at": state.get("StartedAt"),
                    "healthy": healthy,
                    "critical": is_critical,
                }
            )
            if not running:
                if is_critical:
                    critical_down = True
                else:
                    noncritical_issue = True
            elif not healthy:
                noncritical_issue = True
        except Exception as exc:  # noqa: BLE001 - missing container, etc.
            # NotFound or any inspect error -> treat as missing, never raise.
            items.append(
                {
                    "name": name,
                    "found": False,
                    "status": "missing",
                    "image": None,
                    "started_at": None,
                    "healthy": False,
                    "critical": is_critical,
                }
            )
            if is_critical:
                critical_down = True
            else:
                noncritical_issue = True
            errors.append(f"{name}: not found ({type(exc).__name__})")

    if critical_down:
        status = "down"
    elif noncritical_issue:
        status = "degraded"
    else:
        status = "healthy"

    return {"status": status, "items": items, "errors": errors}


# --- Section: postgres -------------------------------------------------------
def _check_postgres() -> dict:
    settings = get_settings()
    started = time.perf_counter()
    try:
        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "status": "healthy",
            "connection": "ok",
            "latency_ms": latency_ms,
            "errors": [],
        }
    except Exception as exc:  # noqa: BLE001 - never echo password/DSN
        return {
            "status": "down",
            "connection": "failed",
            "latency_ms": None,
            "errors": [f"postgres connection failed: {type(exc).__name__}"],
        }


# --- Section: gold tables ----------------------------------------------------
def _detect_date_column(cur, table: str) -> str | None:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table,),
    )
    present = {row[0] for row in cur.fetchall()}
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in present:
            return candidate
    return None


def _check_one_gold_table(cur, table: str) -> dict:
    # ``table`` comes only from GOLD_TABLE_WHITELIST.
    if table not in GOLD_TABLE_WHITELIST:  # defensive; should never happen
        raise ValueError("table not whitelisted")

    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    row_count = cur.fetchone()[0]

    info: dict = {"table": table, "row_count": row_count}

    date_col = _detect_date_column(cur, table)
    if date_col is None:
        # row_count known but freshness unknown -> degraded per spec.
        info["freshness_unknown"] = True
        info["status"] = "degraded"
        if row_count == 0:
            info["empty"] = True
        return info

    # date_col is guaranteed to be one of DATE_COLUMN_CANDIDATES -> safe.
    cur.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"')
    min_val, max_val = cur.fetchone()
    info["date_column"] = date_col
    info["min"] = str(min_val) if min_val is not None else None
    info["max"] = str(max_val) if max_val is not None else None
    info["freshness_unknown"] = False
    if row_count == 0:
        info["empty"] = True
        info["status"] = "degraded"
    else:
        info["status"] = "healthy"
    return info


def _check_gold_tables() -> dict:
    settings = get_settings()
    errors: list[str] = []
    tables: list[dict] = []

    try:
        conn = _pg_connect(settings)
    except Exception as exc:  # noqa: BLE001 - PG unavailable -> section down
        return {
            "status": "down",
            "tables": [],
            "errors": [f"postgres unavailable: {type(exc).__name__}"],
        }

    try:
        for table in GOLD_TABLE_WHITELIST:
            try:
                with conn.cursor() as cur:
                    tables.append(_check_one_gold_table(cur, table))
            except Exception as exc:  # noqa: BLE001 - one table must not stop others
                tables.append(
                    {
                        "table": table,
                        "status": "degraded",
                        "error": str(exc),
                    }
                )
                errors.append(f"{table}: {type(exc).__name__}")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    statuses = {t.get("status") for t in tables}
    if errors or "degraded" in statuses or not tables:
        status = "degraded"
    elif statuses == {"healthy"}:
        status = "healthy"
    else:
        status = "degraded"

    return {"status": status, "tables": tables, "errors": errors}


# --- Section: kafka ----------------------------------------------------------
def _check_kafka() -> dict:
    settings = get_settings()
    errors: list[str] = []
    topics_info: list[dict] = []
    recommended: list[str] = []

    if kafka is None:
        return {
            "status": "down",
            "topics": [],
            "errors": ["kafka client not available"],
            "recommended_actions": ["Install kafka-python and verify broker connectivity"],
        }

    bootstrap = settings.kafka_bootstrap_servers
    consumer = None
    try:
        consumer = kafka.KafkaConsumer(
            bootstrap_servers=bootstrap,
            request_timeout_ms=5000,
            consumer_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        cluster_topics = set(consumer.topics() or [])
    except Exception as exc:  # noqa: BLE001 - broker unreachable -> section down
        if consumer is not None:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass
        return {
            "status": "down",
            "topics": [],
            "errors": [f"kafka broker unreachable: {type(exc).__name__}"],
            "recommended_actions": [
                "Check Kafka container and broker connectivity",
            ],
        }

    missing = False
    try:
        for topic in KAFKA_TOPIC_WHITELIST:  # inspect ONLY whitelisted topics
            exists = topic in cluster_topics
            entry: dict = {"topic": topic, "exists": exists}
            if exists:
                parts = consumer.partitions_for_topic(topic) or set()
                entry["partitions"] = sorted(parts)
                try:
                    tps = [kafka.TopicPartition(topic, p) for p in parts]
                    end = consumer.end_offsets(tps) if tps else {}
                    entry["latest_offsets"] = {
                        tp.partition: off for tp, off in end.items()
                    }
                except Exception as exc:  # noqa: BLE001
                    entry["latest_offsets"] = None
                    errors.append(f"{topic}: offsets unavailable ({type(exc).__name__})")
            else:
                missing = True
                errors.append(f"{topic}: missing")
            # Consumer-group lag is deferred in v1.
            entry["lag_available"] = False
            topics_info.append(entry)
    finally:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            pass

    recommended.append(
        "Consumer-group lag not computed in v1; wire group offsets to enable lag"
    )
    status = "degraded" if (missing or errors) else "healthy"
    return {
        "status": status,
        "topics": topics_info,
        "errors": errors,
        "recommended_actions": recommended,
    }


# --- Section: logs -----------------------------------------------------------
def _scan_log_lines(text: str) -> list[str]:
    flagged = []
    for line in text.splitlines():
        for pattern in LOG_ERROR_PATTERNS:
            if pattern.lower() in line.lower():
                flagged.append(redact_secrets(line))
                break
    return flagged


def _check_logs() -> dict:
    errors: list[str] = []
    findings: list[dict] = []

    if docker is None:
        return {
            "status": "degraded",
            "recent_errors": [],
            "errors": ["docker SDK not available"],
        }

    try:
        client = docker.from_env()
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "degraded",
            "recent_errors": [],
            "errors": [f"docker socket unavailable: {exc}"],
        }

    tail = min(LOG_TAIL, LOG_TAIL_MAX)
    any_errors = False

    for name in FORJA_CONTAINER_WHITELIST:  # whitelisted containers only
        try:
            container = client.containers.get(name)
            raw = container.logs(tail=tail)
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            raw = redact_secrets(raw)
            flagged = _scan_log_lines(raw)
            excerpt = _truncate(raw, EXCERPT_MAX)
            status = "degraded" if flagged else "healthy"
            if flagged:
                any_errors = True
            findings.append(
                {
                    "container": name,
                    "recent_errors": flagged,
                    "status": status,
                    "excerpt": excerpt,
                }
            )
        except Exception as exc:  # noqa: BLE001 - missing container etc.
            errors.append(f"{name}: logs unavailable ({type(exc).__name__})")

    status = "degraded" if (any_errors or errors) else "healthy"
    return {"status": status, "recent_errors": findings, "errors": errors}


# --- Aggregation -------------------------------------------------------------
SECTION_FUNCS = {
    "containers": _check_containers,
    "postgres": _check_postgres,
    "gold_tables": _check_gold_tables,
    "kafka": _check_kafka,
    "logs": _check_logs,
}


def _calculate_overall_status(sections: dict) -> str:
    def st(name: str) -> str:
        return sections.get(name, {}).get("status", "skipped")

    # "down" only for CRITICAL dependency failures.
    if st("postgres") == "down" or st("kafka") == "down" or st("containers") == "down":
        return "down"
    if any(s.get("status") == "degraded" for s in sections.values()):
        return "degraded"
    return "healthy"


def _summarize(overall: str) -> str:
    return {
        "healthy": "All checked pipeline sections are healthy.",
        "degraded": "Pipeline is degraded: one or more sections reported warnings or partial failures.",
        "down": "Pipeline is down: a critical dependency (Postgres, Kafka, or a critical container) is failing.",
    }[overall]


def _recommended_actions(sections: dict) -> list[str]:
    actions: list[str] = []
    c = sections.get("containers", {})
    if c.get("status") in ("down", "degraded"):
        bad = [i["name"] for i in c.get("items", []) if not i.get("healthy")]
        if bad:
            actions.append(f"Inspect container(s): {', '.join(bad)} (read-only).")
    if sections.get("postgres", {}).get("status") == "down":
        actions.append("Check Postgres connectivity/credentials for the gold (snrt_stats) DB.")
    gt = sections.get("gold_tables", {})
    if gt.get("status") == "down":
        actions.append("Gold tables unreachable: Postgres is unavailable.")
    elif gt.get("status") == "degraded":
        actions.append("Review empty/stale/unknown-freshness gold tables.")
    k = sections.get("kafka", {})
    if k.get("status") == "down":
        actions.append("Check Kafka container and broker connectivity.")
    elif k.get("status") == "degraded":
        actions.append("Review missing Kafka topics / enable consumer-group lag.")
    if sections.get("logs", {}).get("status") == "degraded":
        actions.append("Recent error log lines detected; review container logs.")
    # Surface section-level recommended_actions (e.g. kafka lag note).
    for sec in sections.values():
        for extra in sec.get("recommended_actions", []) or []:
            if extra not in actions:
                actions.append(extra)
    return actions


def _run(sections: list[str] | None) -> dict:
    requested = list(sections) if sections else list(ALL_SECTIONS)
    out_sections: dict = {}
    for name in ALL_SECTIONS:
        if name in requested:
            result = safe_section(name, SECTION_FUNCS[name])
            # Guarantee the full required sub-shape even on fallback.
            out_sections[name] = {**_section_defaults(name), **result}
        else:
            out_sections[name] = _section_defaults(name)  # not requested -> skipped

    overall = _calculate_overall_status(
        {k: v for k, v in out_sections.items() if v.get("status") != "skipped"}
    )
    return {
        "overall_status": overall,
        "summary": _summarize(overall),
        "checked_at": _now_iso(),
        "sections": out_sections,
        "recommended_actions": _recommended_actions(out_sections),
    }


# --- Tool --------------------------------------------------------------------
@tool(args_schema=GetPipelineStatusInput)
def get_pipeline_status(sections: list[PipelineSection] | None = None) -> dict:
    """Return a READ-ONLY Forja pipeline health report grouped by section.

    Sections: containers, postgres, gold_tables, kafka, logs. If ``sections`` is
    omitted, all are checked; sections not requested are reported as "skipped".
    The tool never mutates any resource and only inspects whitelisted
    containers, topics and gold tables.
    """
    return _run(sections)
