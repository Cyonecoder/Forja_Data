from typing import Any, Literal, NotRequired, TypedDict
from langgraph.graph.message import add_messages
from typing_extensions import Annotated


class ApprovalReq(TypedDict):
    toolname: str
    toolargs: dict[str, Any]
    risktier: Literal["WRITE", "DANGER"]
    rationale: str
    sideeffects: list[str]
    estimatedruntimes: int | None
    requiresdoubleconfirm: bool


class ApprovalDec(TypedDict):
    verdict: Literal["approve", "reject", "edit"]
    editedargs: NotRequired[dict[str, Any] | None]
    comment: NotRequired[str | None]


class ToolAuditEntry(TypedDict):
    toolname: str
    toolargs: dict[str, Any]
    risktier: str
    status: str
    durations: float
    approvedby: str | None
    approvallatencys: float | None
    stdoutexcerpt: str
    errormessage: str | None


class GuardrailFinding(TypedDict):
    stage: str
    rule: str
    severity: str
    detail: str
    at: str


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    user_id: str
    thread_id: str
    pendingapproval: ApprovalReq | None
    approvaldecision: ApprovalDec | None
    refusal: str | None
    toolcallsaudit: list[ToolAuditEntry]
    guardrailfindings: list[GuardrailFinding]
    runmetadata: dict[str, Any]
    sessioncontext: dict[str, Any]