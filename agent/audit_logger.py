"""
audit_logger.py
===============
Layer 5 — Append-only audit logging.

Logs session activity WITHOUT storing:
- Raw question text (privacy)
- Actual data returned (avoid logs becoming exfiltration vector)
- LLM response content

Stores only:
- Timestamp
- Session ID
- Question hash (SHA-256, non-reversible)
- Template matched or BLOCKED
- Output filter result
- Response latency
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOG_FILE = Path(os.environ.get("AGENT_AUDIT_LOG", "agent_audit.log"))


def _hash_question(question: str) -> str:
    """SHA-256 hash of the question — non-reversible, for dedup/pattern detection only."""
    return hashlib.sha256(question.encode()).hexdigest()[:16]


def log_event(
    session_id: str,
    question: str,
    template_matched: str,      # template name, "BLOCKED", "NO_MATCH", or "GREETING"
    output_filter_result: str,  # "PASS" or "BLOCKED"
    latency_ms: float,
    error: str = None,
):
    """Append one audit event to the log file."""
    event = {
        "timestamp":           datetime.now(tz=timezone.utc).isoformat(),
        "session_id":          session_id,
        "question_hash":       _hash_question(question),
        "question_length":     len(question),
        "template_matched":    template_matched,
        "output_filter":       output_filter_result,
        "latency_ms":          round(latency_ms, 2),
        "error":               error,
    }

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def log_blocked_input(session_id: str, question: str, reason: str):
    """Log an input that was blocked before reaching the LLM."""
    event = {
        "timestamp":        datetime.now(tz=timezone.utc).isoformat(),
        "session_id":       session_id,
        "question_hash":    _hash_question(question),
        "question_length":  len(question),
        "template_matched": "BLOCKED_INPUT",
        "block_reason":     reason[:100],   # truncate, don't store full message
        "output_filter":    "N/A",
        "latency_ms":       0,
        "error":            None,
    }
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def get_session_stats(session_id: str) -> dict:
    """
    Read audit log and return stats for a given session.
    Used for rate limiting — how many questions has this session asked?
    """
    if not LOG_FILE.exists():
        return {"total": 0, "blocked": 0}

    total = 0
    blocked = 0
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("session_id") == session_id:
                    total += 1
                    if "BLOCKED" in str(event.get("template_matched", "")):
                        blocked += 1
            except json.JSONDecodeError:
                continue

    return {"total": total, "blocked": blocked}
