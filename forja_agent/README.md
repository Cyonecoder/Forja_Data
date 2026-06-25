# Forja Agent

Initial skeleton for the SNRT Forja Agent based on the design documentation.

## Included
- FastAPI API
- LangGraph minimal graph
- AgentState typed schema
- Initial READ tool: `get_pipeline_status`
- Dev compose file

## Run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

## Test
```bash
curl -X POST http://127.0.0.1:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"Is the pipeline healthy?","user_id":"leila","thread_id":"t1"}'
```

## WP2 — Pipeline health (READ-ONLY)

`get_pipeline_status` is a strictly read-only health checker. It inspects only
whitelisted Docker containers, Kafka topics and gold tables (see
`forja_agent/config/whitelists.py`) and never restarts containers, runs
DDL/DML, or publishes to Kafka.

Sections: `containers`, `postgres`, `gold_tables`, `kafka`, `logs`
(omit `sections` to check all; unrequested sections are reported as `skipped`).

### Smoke test (manual)
```bash
curl -X POST http://127.0.0.1:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"Is the pipeline healthy?","user_id":"leila","thread_id":"t1"}'
```
> Note: the HTTP body uses `user_id` / `thread_id` (the `ChatRequest` schema was
> intentionally unchanged in WP0).

Expected: the supervisor calls `get_pipeline_status`; the JSON report contains
`overall_status` (`healthy` | `degraded` | `down`), per-section detail, and
`recommended_actions`; the assistant's answer summarizes the report.

The gold tables live in the external `snrt_stats` Postgres (see root
`tests/test_pipeline.py`). Set `PG_HOST` / `PG_PASSWORD` in `.env` to point at the
real read-only DB. Consumer-group **lag is deferred in v1** (`lag_available:false`
plus a recommended action).