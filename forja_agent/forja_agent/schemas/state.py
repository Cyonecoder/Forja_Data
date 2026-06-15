from typing import Any, Literal, NotRequired, TypedDict
from langgraph.graph.message import add_messages
from typing_extensions import Annotated

class ApprovalReq(TypedDict):
    tool_name: str
    tool_args: dict[str, Any]
    risk_tier: Literal["WRITE", "DANGER"]
    rationale: str
    side_effects: list[str]
    estimated_runtime_s: int | None
    requires_double_confirm: bool

class ApprovalDec(TypedDict):
    verdict: Literal["approve", "reject", "edit"]
    edited_args: NotRequired[dict[str, Any] | None]
    comment: NotRequired[str | None]

class ToolAuditEntry(TypedDict):
    tool_name: str
    tool_args: dict[str, Any]
    risk_tier: str
    status: str
    duration_s: float
    approved_by: str | None
    approval_latency_s: float | None
    stdout_excerpt: str
    error_message: str | None

class GuardrailFinding(TypedDict):
    stage: str
    rule: str
    severity: str
    detail: str
    timestamp: str

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    user_id: str
    thread_id: str
    pending_approval: ApprovalReq | None
    approval_decision: ApprovalDec | None
    refusal: str | None
    tool_calls_audit: list[ToolAuditEntry]
    guardrail_findings: list[GuardrailFinding]
    run_metadata: dict[str, Any]
    session_context: dict[str, Any]