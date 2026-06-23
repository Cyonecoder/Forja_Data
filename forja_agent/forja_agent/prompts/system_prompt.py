from pathlib import Path

_PROMPT_PATH = Path(__file__).with_name("supervisor_v1.txt")
if not _PROMPT_PATH.exists():
    raise FileNotFoundError(f"Missing supervisor prompt: {_PROMPT_PATH}")
SYSTEM_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")
