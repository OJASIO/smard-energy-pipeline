"""
security.py
===========
Layer 2 — Input Validation
Layer 4 — Output Filtering

All security checks live here so they can be tested independently
and updated without touching agent logic.
"""

import re

# ---------------------------------------------------------------------------
# Layer 2 — Input blocklist
# ---------------------------------------------------------------------------
BLOCKED_INPUT_PATTERNS = [
    # Prompt injection
    "ignore previous", "ignore your instructions", "ignore all previous",
    "forget what", "override your", "disregard your",
    "you are now", "pretend you are", "act as", "roleplay as",
    "jailbreak", "dan mode", "developer mode",

    # System probing
    "system prompt", "show your prompt", "reveal your prompt",
    "what are your instructions", "show me your instructions",
    "list your tools", "show all tables", "show schema",
    "what database", "what tables", "describe table",
    "information_schema",

    # Credential fishing
    "password", "api key", "apikey", "credentials",
    "connection string", "snowflake account", "secret",
    "access token", "private key",

    # SQL injection (lowercase — input is lowercased before checking)
    "drop table", "drop database", "delete from",
    "truncate table", "insert into", "update set",
    "union select", "exec(", "execute(",
    "select * from", "select *", "limit 100",
    "from gold.", "from silver.", "from bronze.",

    # Identity / infrastructure probing
    "what is your name", "who made you", "what model are you",
    "what llm", "what version", "groq", "openai", "anthropic",
    "langchain", "snowflake connector",
]

# Keywords that must appear for a question to be energy-related
# At least one must be present for the question to pass scope check
ENERGY_KEYWORDS = [
    # English
    "renewable", "energy", "power", "electricity", "generation",
    "demand", "consumption", "forecast", "anomaly", "anomalies",
    "wind", "solar", "biomass", "hydro", "coal", "gas", "nuclear",
    "grid", "mwh", "mw", "tso", "smard", "germany", "german",
    "50hertz", "amprion", "tennet", "transnetbw",
    "trend", "share", "percentage", "compare", "comparison",
    "today", "yesterday", "last week", "last month", "last year",
    "recent", "current", "latest", "highest", "lowest",
    # German
    "erneuerbar", "energie", "strom", "stromerzeugung", "erzeugung",
    "nachfrage", "verbrauch", "prognose", "vorhersage",
    "anomalie", "ungewöhnlich", "wind", "solar", "biomasse",
    "netz", "anteil", "prozent", "vergleich", "trend",
    "heute", "gestern", "letzten monat", "letztes jahr", "letzte woche",
    "aktuell", "aktuell", "höchste", "niedrigste", "deutschland",
    "erneuerbarer", "energien", "wie war", "wie viel", "wie hat",
    "welche", "wann", "strom erzeugt", "stromnachfrage",
]

GENERIC_SAFE_QUESTIONS = [
    "hello", "hi", "help", "what can you do",
    "what can you help", "how does this work",
]


def validate_input(question: str) -> tuple:
    """
    Returns (is_safe: bool, reason: str).
    Checks blocklist then scope before passing to agent.
    """
    q_lower = question.lower().strip()

    # Empty input
    if not q_lower:
        return False, "Please ask a question about the German energy grid."

    # Too long — potential prompt stuffing
    if len(question) > 500:
        return False, "Your question is too long. Please keep questions under 500 characters."

    # Blocklist check
    for pattern in BLOCKED_INPUT_PATTERNS:
        if pattern in q_lower:
            return False, (
                "I can only answer questions about the German electricity grid — "
                "renewable generation, demand, forecasts, and anomalies."
            )

    # Generic safe questions (greetings, capability questions)
    for safe in GENERIC_SAFE_QUESTIONS:
        if safe in q_lower:
            return True, "safe"

    # Scope check — must contain at least one energy keyword
    has_energy_keyword = any(kw in q_lower for kw in ENERGY_KEYWORDS)
    if not has_energy_keyword:
        return False, (
            "I can only answer questions about the German electricity grid. "
            "Try asking about renewable generation, electricity demand, "
            "forecasts, or anomalies."
        )

    return True, "safe"


# ---------------------------------------------------------------------------
# Layer 4 — Output filter
# ---------------------------------------------------------------------------

# Exact strings that should never appear in agent output
SENSITIVE_OUTPUT_STRINGS = [
    "SmardPipeline",
    "qg17675",
    "OJASINDULKAR",
    "ojasindulkar",
    "LLM_AGENT_READONLY",
    "TRANSFORMER",
    "snowflake.connector",
    "langchain",
    "ChatGroq",
    "COMPUTE_WH",
    "SMARD_PROD",
    "SMARD_DEV",
    "data-management-2",
    "europe-west3",
]

# Regex patterns for sensitive data formats
SENSITIVE_OUTPUT_PATTERNS = [
    r"gsk_[a-zA-Z0-9]{20,}",           # Groq API key format
    r"SELECT\s+.+\s+FROM",              # raw SQL
    r"DROP\s+TABLE",                    # destructive SQL
    r"password\s*[:=]\s*\S+",          # password assignment
    r"account\s*[:=]\s*qg\d+",         # Snowflake account
]


def filter_output(response: str) -> tuple:
    """
    Returns (is_safe: bool, cleaned_response: str).
    Blocks any response containing sensitive infrastructure details.
    """
    # Exact string check
    for sensitive in SENSITIVE_OUTPUT_STRINGS:
        if sensitive in response:
            return False, (
                "I encountered an issue generating that response. "
                "Please rephrase your question."
            )

    # Regex pattern check
    for pattern in SENSITIVE_OUTPUT_PATTERNS:
        if re.search(pattern, response, re.IGNORECASE):
            return False, (
                "I encountered an issue generating that response. "
                "Please rephrase your question."
            )

    return True, response
