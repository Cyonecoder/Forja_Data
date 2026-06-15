from fastapi import FastAPI
from langchain_core.messages import HumanMessage

from forja_agent.graph.builder import build_graph
from forja_agent.schemas.api import ChatRequest, ChatResponse

app = FastAPI(title="forja-agent-api")
graph = build_graph()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest):
    state = {
        "messages": [HumanMessage(content=payload.message)],
        "user_id": payload.user_id,
        "thread_id": payload.thread_id,
        "pending_approval": None,
        "approval_decision": None,
        "refusal": None,
        "tool_calls_audit": [],
        "guardrail_findings": [],
        "run_metadata": {},
        "session_context": {},
    }
    result = graph.invoke(state)
    last_message = result["messages"][-1]
    answer = getattr(last_message, "content", str(last_message))
    return ChatResponse(
        thread_id=payload.thread_id,
        answer=answer if answer else "Tool executed successfully.",
        pending_approval=result.get("pending_approval"),
    )