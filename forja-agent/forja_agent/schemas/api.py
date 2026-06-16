from pydantic import BaseModel


class ChatRequest(BaseModel):
    message: str
    userid: str = "leila"
    threadid: str = "thread-dev-001"


class ChatResponse(BaseModel):
    threadid: str
    answer: str
    pendingapproval: dict | None = None