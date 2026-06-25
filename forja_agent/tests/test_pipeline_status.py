"""WP2 tests for the READ-ONLY pipeline health checker.

Everything is mocked: no real Docker, Postgres or Kafka is contacted. Backend
libraries are patched at the module level (``pipeline.docker`` / ``pipeline.psycopg``
/ ``pipeline.kafka``) so the suite runs even when those libs are not installed.
"""
import json
from collections import namedtuple

import pytest
from pydantic import ValidationError

import forja_agent.tools.pipeline as pipeline
from forja_agent.tools.pipeline import (
    GetPipelineStatusInput,
    get_pipeline_status,
    redact_secrets,
)
from forja_agent.config.whitelists import (
    FORJA_CONTAINER_WHITELIST,
    GOLD_TABLE_WHITELIST,
    KAFKA_TOPIC_WHITELIST,
)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class FakeNotFound(Exception):
    pass


class FakeContainer:
    def __init__(self, status="running", health=None, image="img:latest", logs=b""):
        self.status = status
        self.attrs = {
            "State": {
                "Status": status,
                "StartedAt": "2026-01-01T00:00:00Z",
                "Health": ({"Status": health} if health else {}),
            },
            "Config": {"Image": image},
        }
        self.image = type("Img", (), {"tags": [image]})()
        self._logs = logs

    def logs(self, tail=50):
        return self._logs


class FakeContainers:
    def __init__(self, mapping):
        self.mapping = mapping
        self.requested = []

    def get(self, name):
        self.requested.append(name)
        if name in self.mapping:
            return self.mapping[name]
        raise FakeNotFound(name)


class FakeDockerClient:
    def __init__(self, mapping):
        self.containers = FakeContainers(mapping)


class FakeDockerModule:
    """Stand-in for the ``docker`` module."""

    def __init__(self, mapping=None, from_env_fails=False):
        self._mapping = mapping or {}
        self._from_env_fails = from_env_fails
        self.client = None

    def from_env(self):
        if self._from_env_fails:
            raise RuntimeError("cannot connect to docker socket")
        self.client = FakeDockerClient(self._mapping)
        return self.client


def _all_running_docker():
    mapping = {name: FakeContainer(status="running") for name in FORJA_CONTAINER_WHITELIST}
    return FakeDockerModule(mapping)


# ---- psycopg fake ---------------------------------------------------------- #
class FakeCursor:
    def __init__(self, handler):
        self.handler = handler

    def execute(self, query, params=None):
        self._row, self._rows = self.handler(query, params)

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, handler):
        self.handler = handler
        self.closed = False

    def cursor(self):
        return FakeCursor(self.handler)

    def close(self):
        self.closed = True


class FakePsycopg:
    def __init__(self, handler=None, connect_fails=False, connect_error=None):
        self.handler = handler or default_pg_handler
        self.connect_fails = connect_fails
        self.connect_error = connect_error

    def connect(self, **kwargs):
        if self.connect_fails:
            raise (self.connect_error or RuntimeError("connection refused"))
        return FakeConn(self.handler)


def default_pg_handler(query, params=None):
    q = query
    if "SELECT 1" in q:
        return (1,), []
    if "information_schema" in q:
        return None, [("report_date",)]
    if "COUNT(*)" in q:
        return (42,), []
    if "MIN(" in q:
        return ("2026-06-01", "2026-06-25"), []
    return (None,), []


# ---- kafka fake ------------------------------------------------------------ #
_TP = namedtuple("TP", "topic partition")


class FakeConsumer:
    def __init__(self, module):
        self._m = module

    def topics(self):
        return set(self._m.cluster_topics)

    def partitions_for_topic(self, topic):
        return self._m.partitions.get(topic, {0})

    def end_offsets(self, tps):
        return {tp: self._m.offsets.get((tp.topic, tp.partition), 100) for tp in tps}

    def close(self):
        pass


class FakeKafkaModule:
    def __init__(self, cluster_topics=None, construct_fails=False):
        self.cluster_topics = (
            cluster_topics if cluster_topics is not None else list(KAFKA_TOPIC_WHITELIST)
        )
        self.construct_fails = construct_fails
        self.partitions = {}
        self.offsets = {}

    def KafkaConsumer(self, **kwargs):
        if self.construct_fails:
            raise RuntimeError("NoBrokersAvailable")
        return FakeConsumer(self)

    def TopicPartition(self, topic, partition):
        return _TP(topic, partition)


# --------------------------------------------------------------------------- #
# 1 & 2: input validation
# --------------------------------------------------------------------------- #
def test_valid_sections_accepted():
    model = GetPipelineStatusInput(sections=["containers", "kafka", "postgres"])
    assert model.sections == ["containers", "kafka", "postgres"]
    assert GetPipelineStatusInput().sections is None  # omitted => all


def test_invalid_section_rejected():
    with pytest.raises(ValidationError):
        GetPipelineStatusInput(sections=["containers", "bogus_section"])


# --------------------------------------------------------------------------- #
# 3: full required output shape
# --------------------------------------------------------------------------- #
def test_full_required_shape(monkeypatch):
    # No backends available -> every section still present with required keys.
    monkeypatch.setattr(pipeline, "docker", None)
    monkeypatch.setattr(pipeline, "psycopg", None)
    monkeypatch.setattr(pipeline, "kafka", None)

    result = get_pipeline_status.invoke({})

    assert set(result) >= {
        "overall_status",
        "summary",
        "checked_at",
        "sections",
        "recommended_actions",
    }
    assert result["overall_status"] in {"healthy", "degraded", "down", "skipped"}
    secs = result["sections"]
    assert set(secs) == {"containers", "postgres", "gold_tables", "kafka", "logs"}
    assert set(secs["containers"]) >= {"status", "items", "errors"}
    assert set(secs["postgres"]) >= {"status", "connection", "errors"}
    assert set(secs["gold_tables"]) >= {"status", "tables", "errors"}
    assert set(secs["kafka"]) >= {"status", "topics", "errors"}
    assert set(secs["logs"]) >= {"status", "recent_errors", "errors"}


# --------------------------------------------------------------------------- #
# 4: subset -> other sections "skipped"
# --------------------------------------------------------------------------- #
def test_subset_marks_others_skipped(monkeypatch):
    monkeypatch.setattr(pipeline, "docker", _all_running_docker())
    result = get_pipeline_status.invoke({"sections": ["containers"]})
    secs = result["sections"]
    assert secs["containers"]["status"] != "skipped"
    for other in ("postgres", "gold_tables", "kafka", "logs"):
        assert secs[other]["status"] == "skipped"


# --------------------------------------------------------------------------- #
# 5: containers use the whitelist only
# --------------------------------------------------------------------------- #
def test_containers_use_whitelist_only(monkeypatch):
    fake = _all_running_docker()
    monkeypatch.setattr(pipeline, "docker", fake)

    get_pipeline_status.invoke({"sections": ["containers"]})

    requested = fake.client.containers.requested
    # Only whitelisted names are inspected, and every whitelisted name is checked.
    assert set(requested) == set(FORJA_CONTAINER_WHITELIST)
    assert all(name in FORJA_CONTAINER_WHITELIST for name in requested)


# --------------------------------------------------------------------------- #
# 6: docker unavailable degrades the containers section (never raises)
# --------------------------------------------------------------------------- #
def test_docker_unavailable_degrades(monkeypatch):
    monkeypatch.setattr(pipeline, "docker", FakeDockerModule(from_env_fails=True))
    result = get_pipeline_status.invoke({"sections": ["containers"]})
    assert result["sections"]["containers"]["status"] == "degraded"
    assert result["sections"]["containers"]["errors"]


# --------------------------------------------------------------------------- #
# 7: postgres success
# --------------------------------------------------------------------------- #
def test_postgres_success(monkeypatch):
    monkeypatch.setattr(pipeline, "psycopg", FakePsycopg())
    result = get_pipeline_status.invoke({"sections": ["postgres"]})
    pg = result["sections"]["postgres"]
    assert pg["status"] == "healthy"
    assert pg["connection"] == "ok"
    assert pg["latency_ms"] is not None


# --------------------------------------------------------------------------- #
# 8: postgres failure -> down, and the password is never leaked
# --------------------------------------------------------------------------- #
def test_postgres_failure_is_down_and_safe(monkeypatch):
    secret = "sup3r-s3cret-pw"
    monkeypatch.setattr(
        pipeline,
        "psycopg",
        FakePsycopg(connect_fails=True, connect_error=RuntimeError(f"auth failed password={secret}")),
    )
    result = get_pipeline_status.invoke({"sections": ["postgres"]})
    pg = result["sections"]["postgres"]
    assert pg["status"] == "down"
    assert pg["connection"] == "failed"
    assert secret not in json.dumps(pg)
    assert "password" not in json.dumps(pg).lower()


# --------------------------------------------------------------------------- #
# 9: one gold table failing must not crash the others
# --------------------------------------------------------------------------- #
def test_one_gold_table_failure_isolated(monkeypatch):
    bad_table = "gold_snrt_engagement"

    def handler(query, params=None):
        if bad_table in query:
            raise RuntimeError("relation blew up")
        return default_pg_handler(query, params)

    monkeypatch.setattr(pipeline, "psycopg", FakePsycopg(handler=handler))
    result = get_pipeline_status.invoke({"sections": ["gold_tables"]})
    gt = result["sections"]["gold_tables"]

    assert len(gt["tables"]) == len(GOLD_TABLE_WHITELIST)  # all attempted
    failed = [t for t in gt["tables"] if t["table"] == bad_table][0]
    assert failed["status"] == "degraded"
    assert gt["status"] == "degraded"


# --------------------------------------------------------------------------- #
# 10: kafka partial failure (missing topic) degrades the section
# --------------------------------------------------------------------------- #
def test_kafka_missing_topic_degrades(monkeypatch):
    # Broker reachable but one whitelisted topic is missing.
    present = list(KAFKA_TOPIC_WHITELIST)[:-1]
    monkeypatch.setattr(pipeline, "kafka", FakeKafkaModule(cluster_topics=present))
    result = get_pipeline_status.invoke({"sections": ["kafka"]})
    k = result["sections"]["kafka"]
    assert k["status"] == "degraded"
    assert any(t["exists"] is False for t in k["topics"])


# --------------------------------------------------------------------------- #
# 11: logs redact secrets
# --------------------------------------------------------------------------- #
def test_logs_redact_secrets(monkeypatch):
    secret = "supersecretvalue"
    jwt = "eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM"
    log = (
        f"ERROR DB_PASSWORD={secret} could not connect\n"
        f"INFO dsn=postgresql://user:{secret}@db:5432/snrt\n"
        f"WARN token {jwt}\n"
    ).encode("utf-8")
    mapping = {FORJA_CONTAINER_WHITELIST[0]: FakeContainer(logs=log)}
    monkeypatch.setattr(pipeline, "docker", FakeDockerModule(mapping=mapping))

    result = get_pipeline_status.invoke({"sections": ["logs"]})
    blob = json.dumps(result["sections"]["logs"])
    assert secret not in blob
    assert jwt not in blob
    assert "***REDACTED***" in blob or "***REDACTED_JWT***" in blob


def test_redact_secrets_unit():
    assert "***REDACTED***" in redact_secrets("API_KEY=abc123")
    assert "abc123" not in redact_secrets("API_KEY=abc123")
    assert "***REDACTED***" in redact_secrets("postgresql://u:p@h:5432/db")


# --------------------------------------------------------------------------- #
# 12: logs truncate long excerpts
# --------------------------------------------------------------------------- #
def test_logs_truncate(monkeypatch):
    long_log = ("ERROR " + "x" * 5000).encode("utf-8")
    mapping = {FORJA_CONTAINER_WHITELIST[0]: FakeContainer(logs=long_log)}
    monkeypatch.setattr(pipeline, "docker", FakeDockerModule(mapping=mapping))

    result = get_pipeline_status.invoke({"sections": ["logs"]})
    findings = result["sections"]["logs"]["recent_errors"]
    assert findings
    for f in findings:
        assert len(f["excerpt"]) <= 2000


# --------------------------------------------------------------------------- #
# 13/14/15: overall status calculation
# --------------------------------------------------------------------------- #
def test_overall_healthy(monkeypatch):
    monkeypatch.setattr(pipeline, "docker", _all_running_docker())
    monkeypatch.setattr(pipeline, "psycopg", FakePsycopg())
    monkeypatch.setattr(pipeline, "kafka", FakeKafkaModule())

    result = get_pipeline_status.invoke({"sections": ["containers", "postgres", "kafka"]})
    assert result["sections"]["containers"]["status"] == "healthy"
    assert result["sections"]["postgres"]["status"] == "healthy"
    assert result["sections"]["kafka"]["status"] == "healthy"
    assert result["overall_status"] == "healthy"


def test_overall_degraded(monkeypatch):
    # A NON-critical container stopped -> containers degraded -> overall degraded.
    mapping = {name: FakeContainer(status="running") for name in FORJA_CONTAINER_WHITELIST}
    mapping["forja_minio"] = FakeContainer(status="exited")  # non-critical
    monkeypatch.setattr(pipeline, "docker", FakeDockerModule(mapping=mapping))

    result = get_pipeline_status.invoke({"sections": ["containers"]})
    assert result["sections"]["containers"]["status"] == "degraded"
    assert result["overall_status"] == "degraded"


def test_overall_down(monkeypatch):
    # Kafka broker unreachable -> kafka section down -> overall down.
    monkeypatch.setattr(pipeline, "kafka", FakeKafkaModule(construct_fails=True))
    result = get_pipeline_status.invoke({"sections": ["kafka"]})
    assert result["sections"]["kafka"]["status"] == "down"
    assert result["overall_status"] == "down"


def test_overall_down_critical_container(monkeypatch):
    # A CRITICAL container missing -> containers down -> overall down.
    mapping = {name: FakeContainer(status="running") for name in FORJA_CONTAINER_WHITELIST}
    del mapping["forja_kafka"]  # critical, now missing
    monkeypatch.setattr(pipeline, "docker", FakeDockerModule(mapping=mapping))

    result = get_pipeline_status.invoke({"sections": ["containers"]})
    assert result["sections"]["containers"]["status"] == "down"
    assert result["overall_status"] == "down"


# --------------------------------------------------------------------------- #
# 16: result is json.dumps-able
# --------------------------------------------------------------------------- #
def test_result_is_json_serializable(monkeypatch):
    monkeypatch.setattr(pipeline, "docker", _all_running_docker())
    monkeypatch.setattr(pipeline, "psycopg", FakePsycopg())
    monkeypatch.setattr(pipeline, "kafka", FakeKafkaModule())

    result = get_pipeline_status.invoke({})
    dumped = json.dumps(result)  # must not raise
    assert isinstance(dumped, str)
