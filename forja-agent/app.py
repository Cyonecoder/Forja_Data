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
        "user_id": payload.userid,
        "thread_id": payload.threadid,
        "pendingapproval": None,
        "approvaldecision": None,
        "refusal": None,
        "toolcallsaudit": [],
        "guardrailfindings": [],
        "runmetadata": {},
        "sessioncontext": {},
    }

    result = graph.invoke(state)
    last_message = result["messages"][-1]
    answer = getattr(last_message, "content", str(last_message))

    return ChatResponse(
        threadid=payload.threadid,
        answer=answer if answer else "Tool executed successfully.",
        pendingapproval=result.get("pendingapproval"),
    )