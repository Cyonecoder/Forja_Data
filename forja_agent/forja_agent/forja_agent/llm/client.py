"""
Client LLM centralisé pour le Forja Agent.

Permet de switcher entre Ollama et vLLM via la config uniquement.
Dev utilise Ollama local, Prod utilise vLLM.
"""

from langchain_openai import ChatOpenAI
from config.settings import settings


def create_supervisor_model() -> ChatOpenAI:
    """
    Crée le modèle LLM pour le noeud supervisor.
    """
    return ChatOpenAI(
        base_url=settings.supervisor_llm_url,
        model=settings.supervisor_llm_model,
        temperature=0.0,
    )


def create_judge_model() -> ChatOpenAI:
    """
    Crée le modèle LLM pour le noeud judge/guardrail.
    """
    return ChatOpenAI(
        base_url=settings.judge_llm_url,
        model=settings.judge_llm_model,
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