"""
Graph nodes for the Forja Agent LangGraph state machine.
"""

from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from forja_agent.llm.client import create_supervisor_model
from forja_agent.config.settings import get_settings
from forja_agent.prompts.system_prompt import SYSTEM_PROMPT
from forja_agent.tools.registry import get_all_tools


def get_supervisor_model():
    """Construit le modèle supervisor (local LLM) bindé avec les tools.

    Factory patchable : les tests remplacent
    ``forja_agent.graph.nodes.get_supervisor_model`` pour éviter tout appel
    réseau. Aucun modèle réel n'est construit à l'import.
    """
    return create_supervisor_model(get_settings()).bind_tools(get_all_tools())


def inputguardrail_fn(state):
    """
    Noeud input guardrail.
    Vérifie si la requête est sûre.
    Actuellement un stub — à implémenter avec le judge model.
    """
    return {"refusal": None}


def supervisor_fn(state, config: RunnableConfig | None = None):
    """
    Noeud supervisor : utilise le VRAI LLM pour décider quoi faire.
    Remplace l'ancien if 'pipeline' in user_text.
    Le modèle reçoit le system prompt + les messages + les tools bindés.
    Il décidera soit de :
      - appeler un tool (retourne tool_calls)
      - répondre directement (retourne content)
    """
    model = get_supervisor_model()

    # Construire les messages: system + historial utilisateur
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        *state["messages"],
    ]

    # Appel au modèle LLM
    response = model.invoke(messages)

    return {"messages": [response]}


def toolgate_fn(state):
    """
    Noeud tool gate.
    Vérifie si l'appel d'outil nécessite une approbation.
    Actuellement un stub.
    """
    return {"pending_approval": None}


def toolaudit_fn(state):
    """
    Noeud tool audit.
    Enregistre l'appel d'outil dans l'audit log.
    """
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
        "tool_name": tool_name,
        "tool_args": tool_args,
        "risk_tier": "READ",
        "status": "success",
        "duration_s": 0.0,
        "approved_by": None,
        "approval_latency_s": None,
        "stdout_excerpt": str(content)[:2000],
        "error_message": None,
    }
    return {"tool_calls_audit": state.get("tool_calls_audit", []) + [entry]}


def outputguardrail_fn(state):
    """
    Noeud output guardrail.
    Vérifie que la réponse est sûre.
    Actuellement un stub.
    """
    return {"refusal": None}