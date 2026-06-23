"""
Tests déterministes du graph Forja Agent.

Utilise un fake chat model (FakeMessagesListChatModel) pour éviter de dépendre
d'un vrai LLM : Ollama n'a pas besoin de tourner pour exécuter pytest.
Le supervisor est patché via ``forja_agent.graph.nodes.get_supervisor_model``.
"""
from typing import Any, List

from pydantic import Field
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage

import forja_agent.graph.nodes as nodes
from forja_agent.graph.builder import build_graph, route_after_supervisor
from forja_agent.config.settings import Settings
from forja_agent.llm.client import create_supervisor_model


class ToolBindableFake(FakeMessagesListChatModel):
    """Fake chat model qui supporte bind_tools() et renvoie des messages préréglés."""

    bound_tools: List[Any] = Field(default_factory=list)

    def bind_tools(self, tools, **kwargs):
        self.bound_tools = list(tools)
        return self


def _initial_state(message: str) -> dict:
    return {
        "messages": [HumanMessage(content=message)],
        "user_id": "leila",
        "thread_id": "thread-test-001",
        "pending_approval": None,
        "approval_decision": None,
        "refusal": None,
        "tool_calls_audit": [],
        "guardrail_findings": [],
        "run_metadata": {},
        "session_context": {},
    }


def test_graph_basic_flow(monkeypatch):
    fake = ToolBindableFake(responses=[AIMessage(content="Hello from Forja Agent")])
    monkeypatch.setattr(nodes, "get_supervisor_model", lambda: fake)

    graph = build_graph()
    result = graph.invoke(_initial_state("Hi there, who are you?"))

    last = result["messages"][-1]
    assert last.content == "Hello from Forja Agent"
    # Aucun tool ne doit s'exécuter sur un message non-pipeline.
    assert result["tool_calls_audit"] == []


def test_graph_with_tool_call(monkeypatch):
    fake = ToolBindableFake(
        responses=[
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
            ),
            AIMessage(content="Pipeline status checked successfully."),
        ]
    )
    monkeypatch.setattr(nodes, "get_supervisor_model", lambda: fake)

    graph = build_graph()
    result = graph.invoke(_initial_state("Is the Forja pipeline healthy?"))

    last = result["messages"][-1]
    assert last.content == "Pipeline status checked successfully."

    audit = result["tool_calls_audit"]
    assert len(audit) == 1
    entry = audit[0]
    # L'entrée d'audit utilise les noms de champs snake_case de AgentState.
    assert entry["tool_name"] == "get_pipeline_status"
    assert entry["tool_args"] == {"sections": ["containers", "kafka", "postgres"]}
    assert entry["risk_tier"] == "READ"
    assert entry["status"] == "success"


def test_routes():
    with_tool = _initial_state("anything")
    with_tool["messages"] = [
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "get_pipeline_status",
                    "args": {},
                    "id": "call_1",
                    "type": "tool_call",
                }
            ],
        )
    ]
    assert route_after_supervisor(with_tool) == "toolgate"

    without_tool = _initial_state("anything")
    without_tool["messages"] = [AIMessage(content="Just a plain answer.")]
    assert route_after_supervisor(without_tool) == "outputguardrail"


def test_local_llm_client_config():
    settings = Settings(
        supervisor_llm_url="http://localhost:11434/v1",
        supervisor_llm_model="qwen3:14b",
        supervisor_llm_api_key="ollama",
    )
    model = create_supervisor_model(settings)
    # Construit uniquement ; jamais invoqué (aucun accès réseau dans les tests).
    assert model is not None
