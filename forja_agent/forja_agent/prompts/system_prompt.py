import os
from pathlib import Path

PROMPT_FILE = Path(__file__).parent / "supervisor_v1.txt"

SYSTEM_PROMPT = (
    PROMPT_FILE.read_text(encoding="utf-8")
    if PROMPT_FILE.exists()
    else """
You are Forja Agent, an internal assistant for the SNRT data team.
You help with pipeline status, analytics, and approved tool actions.
Rules: prefer READ tools, do not modify code, be concise.
"""
)