from pydantic import BaseModel

class ChatRequest(BaseModel):
    message: str
    user_id: str = "leila"
    thread_id: str = "thread-dev-001"

class ChatResponse(BaseModel):
    thread_id: str
    answer: str
    pending_approval: dict | None = None