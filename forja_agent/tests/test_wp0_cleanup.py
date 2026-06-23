"""
Tests WP0 cleanup : clés d'état normalisées + construction des modèles
LLM locaux (Ollama/vLLM) avec api_key factice.

Ces tests n'appellent JAMAIS un vrai modèle : ils vérifient seulement
la forme des données et la construction du client. Ollama n'a pas besoin
de tourner pour exécuter pytest.
"""
from langchain_core.messages import AIMessage, ToolMessage

import forja_agent.graph.nodes as nodes
from forja_agent.llm.client import create_supervisor_model, create_judge_model


def test_toolaudit_returns_normalized_state_key():
    """toolaudit_fn doit retourner 'tool_calls_audit' (pas 'toolcallsaudit')."""
    state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "get_pipeline_status",
                        "args": {"sections": ["containers"]},
                        "id": "call_1",
                        "type": "tool_call",
                    }
                ],
            ),
            ToolMessage(content="containers running", tool_call_id="call_1"),
        ],
        "tool_calls_audit": [],
    }

    result = nodes.toolaudit_fn(state)

    assert "tool_calls_audit" in result
    assert "toolcallsaudit" not in result


def test_toolaudit_entry_uses_normalized_field_names():
    """L'entrée d'audit doit utiliser 'tool_name' (pas 'toolname'), etc."""
    state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "get_pipeline_status",
                        "args": {"sections": ["kafka"]},
                        "id": "call_2",
                        "type": "tool_call",
                    }
                ],
            ),
            ToolMessage(content="kafka healthy", tool_call_id="call_2"),
        ],
        "tool_calls_audit": [],
    }

    entry = nodes.toolaudit_fn(state)["tool_calls_audit"][0]

    expected_keys = {
        "tool_name",
        "tool_args",
        "risk_tier",
        "status",
        "duration_s",
        "approved_by",
        "approval_latency_s",
        "stdout_excerpt",
        "error_message",
    }
    assert set(entry.keys()) == expected_keys
    # Aucune ancienne clé ne doit subsister.
    old_keys = {
        "toolname",
        "toolargs",
        "risktier",
        "durations",
        "approvedby",
        "approvallatencys",
        "stdoutexcerpt",
        "errormessage",
    }
    assert not (set(entry.keys()) & old_keys)
    assert entry["tool_name"] == "get_pipeline_status"
    assert entry["tool_args"] == {"sections": ["kafka"]}


def test_create_supervisor_model_local_dummy_api_key():
    """create_supervisor_model() se construit avec une api_key locale factice."""
    model = create_supervisor_model()
    assert model is not None
    # Endpoint local uniquement, pas de cloud.
    assert "openai.com" not in str(model.openai_api_base or "")


def test_create_judge_model_local_dummy_api_key():
    """create_judge_model() se construit avec une api_key locale factice."""
    model = create_judge_model()
    assert model is not None
    assert "openai.com" not in str(model.openai_api_base or "")
