# Forja Agent

Initial skeleton for the SNRT Forja Agent based on the design documentation.

## Included
- FastAPI API
- LangGraph minimal graph
- AgentState typed schema
- Initial READ tool: `getpipelinestatus`
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