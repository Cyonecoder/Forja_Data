"""
Tests déterministes du graph Forja Agent.
Utilise FakeListChatModel pour éviter de dépendre d'un vrai LLM.
"""
from typing import List, Any
import pytest
from pydantic import Field
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from forja_agent.graph.builder import build_graph
from forja_agent.schemas.state import AgentState


class ToolBindableFakeMessagesListChatModel(FakeMessagesListChatModel):
    """Fake chat model for tests that supports bind_tools()."""

    bound_tools: List[Any] = Field(default_factory=list)

    def bind_tools(self, tools, **kwargs):
        self.bound_tools = tools
        return self


def test_graph_basic_flow():
    """
    Test que le graph fonctionne sans vrai LLM.
    FakeListChatModel retourne des réponses précalculées.
    """
    # Stub : modèle fake qui retourne une réponse sans tool_calls
    fake_model = ToolBindableFakeMessagesListChatModel(
        responses=[
            AIMessage(content="Pipeline status is OK. All containers running.")
        ]
    )

    # On patche le modèle du supervisor pour utiliser le fake
    import forja_agent.graph.nodes as nodes
    nodes.SUPERVISOR_MODEL = fake_model

    try:
        graph = build_graph()
        state = {
            "messages": [HumanMessage(content="Is the pipeline healthy?")],
            "user_id": "test_user",
            "thread_id": "test_thread",
            "pending_approval": None,
            "approval_decision": None,
            "refusal": None,
            "tool_calls_audit": [],
            "guardrail_findings": [],
            "run_metadata": {},
            "session_context": {},
        }
        result = graph.invoke(state)
        assert "messages" in result
        assert len(result["messages"]) >= 1
    finally:
        nodes.SUPERVISOR_MODEL = None


def test_graph_with_tool_call():
    """
    Test que le graph gère un tool_call via FakeListChatModel.
    """
    # Stub : modèle qui retourne un tool_call
    fake_model = ToolBindableFakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "get_pipeline_status",
                        "args": {"sections": ["containers", "kafka"]},
                        "id": "call_test_001",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="Voici l'état : containers running, Kafka healthy.",
            ),
        ]
    )

    import forja_agent.graph.nodes as nodes
    nodes.SUPERVISOR_MODEL = fake_model.bind_tools(nodes.all_tools)

    try:
        graph = build_graph()
        state = {
            "messages": [HumanMessage(content="Show me containers and kafka status")],
            "user_id": "test_user",
            "thread_id": "test_thread",
            "pending_approval": None,
            "approval_decision": None,
            "refusal": None,
            "tool_calls_audit": [],
            "guardrail_findings": [],
            "run_metadata": {},
            "session_context": {},
        }
        result = graph.invoke(state)

        # Le graph doit avoir exécuté le tool puis répondu
        assert "messages" in result
        messages = result["messages"]
        # Il doit y avoir : human message, AI tool_call, tool result, AI answer
        assert len(messages) >= 3
    finally:
        nodes.SUPERVISOR_MODEL = None


def test_routes():
    """
    Test les routes du graph (conditional edges).
    """
    from forja_agent.graph.builder import (
        route_after_inputguardrail,
        route_after_supervisor,
        route_after_toolgate,
    )
    from langgraph.graph import END

    # route_after_inputguardrail
    assert route_after_inputguardrail({"refusal": "blocked"}) == END
    assert route_after_inputguardrail({"refusal": None}) == "supervisor"

    # route_after_supervisor avec tool_calls
    mock_ai_with_tool = AIMessage(
        content="",
        tool_calls=[{"name": "test", "args": {}, "id": "1", "type": "tool_call"}],
    )
    state_with_tool = {"messages": [HumanMessage(content="test"), mock_ai_with_tool]}
    assert route_after_supervisor(state_with_tool) == "toolgate"

    # route_after_supervisor sans tool_calls
    mock_ai_no_tool = AIMessage(content="hello")
    state_no_tool = {"messages": [HumanMessage(content="test"), mock_ai_no_tool]}
    assert route_after_supervisor(state_no_tool) == "outputguardrail"

    # route_after_toolgate
    assert route_after_toolgate({}) == "toolnode"
