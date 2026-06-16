import json
from langchain_core.messages import AIMessage


def inputguardrail_fn(state):
    return {"refusal": None}


def supervisor_fn(state):
    last_message = state["messages"][-1]
    user_text = last_message.content.lower()

    if "pipeline" in user_text or "healthy" in user_text or "status" in user_text:
        return {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "get_pipeline_status",
                            "args": {"sections": ["containers", "kafka", "postgres"]},
                            "id": "call_get_pipeline_status_1",
                            "type": "tool_call",
                        }
                    ],
                )
            ]
        }

    return {
        "messages": [
            AIMessage(
                content="For now, the agent skeleton is working locally. I can answer pipeline status requests and basic development flow checks."
            )
        ]
    }


def toolgate_fn(state):
    return {"pendingapproval": None}


def toolaudit_fn(state):
    messages = state.get("messages", [])
    last = messages[-1] if messages else None

    tool_name = "unknown"
    tool_args = {}
    content = getattr(last, "content", "")

    if len(messages) >= 2:
        previous = messages[-2]
        tool_calls = getattr(previous, "tool_calls", []) or []
        if tool_calls:
            tool_name = tool_calls[0].get("name", "unknown")
            tool_args = tool_calls[0].get("args", {})

    entry = {
        "toolname": tool_name,
        "toolargs": tool_args,
        "risktier": "READ",
        "status": "success",
        "durations": 0.0,
        "approvedby": None,
        "approvallatencys": None,
        "stdoutexcerpt": str(content)[:2000],
        "errormessage": None,
    }

    return {"toolcallsaudit": state.get("toolcallsaudit", []) + [entry]}


def outputguardrail_fn(state):
    return {"refusal": None}