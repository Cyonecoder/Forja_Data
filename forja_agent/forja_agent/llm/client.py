"""
Client LLM centralisé pour le Forja Agent.

Permet de switcher entre Ollama et vLLM via la config uniquement.
Dev utilise Ollama local, Prod utilise vLLM.
"""

from langchain_openai import ChatOpenAI
from forja_agent.config.settings import settings


def create_supervisor_model() -> ChatOpenAI:
    """
    Crée le modèle LLM pour le noeud supervisor.

    LOCAL ONLY: pointe vers un endpoint OpenAI-compatible (Ollama/vLLM).
    `api_key` est requis par l'interface du client ChatOpenAI uniquement ;
    c'est une valeur factice ("ollama") pour le endpoint local. Aucune API
    cloud (api.openai.com) n'est utilisée.
    """
    return ChatOpenAI(
        base_url=settings.supervisor_llm_url,
        model=settings.supervisor_llm_model,
        api_key=settings.supervisor_llm_api_key,
        temperature=0.0,
    )


def create_judge_model() -> ChatOpenAI:
    """
    Crée le modèle LLM pour le noeud judge/guardrail.

    LOCAL ONLY: pointe vers un endpoint OpenAI-compatible (Ollama/vLLM).
    `api_key` est requis par l'interface du client ChatOpenAI uniquement ;
    c'est une valeur factice ("ollama") pour le endpoint local. Aucune API
    cloud (api.openai.com) n'est utilisée.
    """
    return ChatOpenAI(
        base_url=settings.judge_llm_url,
        model=settings.judge_llm_model,
        api_key=settings.judge_llm_api_key,
        temperature=0.0,
    )


def get_llm_provider() -> str:
    """
    Retourne le provider LLM actuel basé sur l'URL.
    Retourne 'ollama' ou 'vllm'.
    """
    url = settings.supervisor_llm_url.lower()
    if "vllm" in url or ":8000" in url:
        return "vllm"
    return "ollama"