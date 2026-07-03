"""
energy_agent.py
===============
Energy Intelligence Agent using Groq SDK directly.
No LangChain dependency — direct API calls are more transparent,
easier to debug, and avoid version conflict issues.
"""

import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from groq import Groq

from security import validate_input, filter_output
from audit_logger import log_event, log_blocked_input
from query_templates import run_template
from prompts import build_system_prompt, INTENT_PROMPT, format_context

GROQ_API_KEY = os.environ["GROQ_API_KEY"]
MODEL_NAME   = "llama-3.3-70b-versatile"

MAX_QUESTIONS_PER_SESSION    = 20
MIN_SECONDS_BETWEEN_QUESTIONS = 2

INTENT_TO_TEMPLATE = {
    "FORECAST":        "FORECAST",
    "ANOMALIES":       "ANOMALIES",
    "RENEWABLE_SHARE": "RENEWABLE_SHARE",
    "DEMAND":          "DEMAND",
    "GENERATION":      "GENERATION",
    "COMPARISON":      "COMPARISON",
}


@dataclass
class AgentResponse:
    text: str
    chart_type: Optional[str] = None
    chart_data: Optional[pd.DataFrame] = None
    chart_config: dict = field(default_factory=dict)
    template_used: Optional[str] = None
    latency_ms: float = 0.0
    blocked: bool = False
    block_reason: Optional[str] = None


class EnergyAgent:

    def __init__(self, session_id: str = None):
        self.session_id       = session_id or str(uuid.uuid4())[:8]
        self.question_count   = 0
        self.last_question_time = 0.0
        self.client           = Groq(api_key=GROQ_API_KEY)

    def _call_llm(self, system_prompt: str, user_message: str,
                  max_tokens: int = 1024) -> str:
        response = self.client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_message},
            ],
            max_tokens=max_tokens,
            temperature=0,
        )
        return response.choices[0].message.content.strip()

    def _classify_intent(self, question: str) -> str:
        prompt = INTENT_PROMPT.format(question=question)
        try:
            intent = self.client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=20,
                temperature=0,
            ).choices[0].message.content.strip().upper()
            valid = set(INTENT_TO_TEMPLATE.keys()) | {"GREETING", "UNKNOWN"}
            return intent if intent in valid else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def ask(self, question: str) -> AgentResponse:
        start = time.time()

        # Layer 7 — Rate limiting
        if self.question_count >= MAX_QUESTIONS_PER_SESSION:
            return AgentResponse(
                text="You've reached the session limit (20 questions). Please refresh.",
                blocked=True, block_reason="rate_limit",
            )

        elapsed = time.time() - self.last_question_time
        if elapsed < MIN_SECONDS_BETWEEN_QUESTIONS and self.question_count > 0:
            time.sleep(MIN_SECONDS_BETWEEN_QUESTIONS - elapsed)

        # Layer 2 — Input validation
        is_safe, reason = validate_input(question)
        if not is_safe:
            log_blocked_input(self.session_id, question, reason)
            return AgentResponse(text=reason, blocked=True, block_reason="input_validation")

        # Intent classification
        template_name = self._classify_intent(question)

        if template_name == "GREETING":
            self.question_count += 1
            self.last_question_time = time.time()
            return AgentResponse(
                text=(
                    "Hello! I'm EnergyBot, your German energy grid assistant. "
                    "I can answer questions about:\n"
                    "- Renewable energy generation forecasts\n"
                    "- Grid anomalies and unusual events\n"
                    "- Renewable energy share and trends\n"
                    "- Electricity demand patterns\n"
                    "- Generation by energy source (wind, solar, etc.)\n"
                    "- Year-over-year comparisons\n\n"
                    "What would you like to know?"
                ),
                template_used="GREETING",
            )

        if template_name == "UNKNOWN":
            self.question_count += 1
            self.last_question_time = time.time()
            return AgentResponse(
                text=(
                    "I can only answer questions about the German electricity grid. "
                    "Try asking about renewable generation, demand, forecasts, or anomalies."
                ),
                template_used="UNKNOWN",
            )

        # Run Snowflake query
        try:
            df, chart_type, chart_config = run_template(template_name)
        except Exception as e:
            log_event(self.session_id, question, template_name,
                      "ERROR", (time.time() - start) * 1000, str(e))
            return AgentResponse(
                text="I'm having trouble retrieving data right now. Please try again.",
                template_used=template_name,
            )

        # Build context and call LLM
        context       = format_context(df, template_name)
        system_prompt = build_system_prompt(context)

        try:
            response_text = self._call_llm(system_prompt, question)
        except Exception as e:
            log_event(self.session_id, question, template_name,
                      "LLM_ERROR", (time.time() - start) * 1000, str(e))
            return AgentResponse(
                text="I encountered an error generating a response. Please try again.",
                template_used=template_name,
            )

        # Layer 4 — Output filtering
        is_safe_output, filtered = filter_output(response_text)
        latency_ms = (time.time() - start) * 1000
        log_event(self.session_id, question, template_name,
                  "PASS" if is_safe_output else "BLOCKED", latency_ms)

        self.question_count += 1
        self.last_question_time = time.time()

        return AgentResponse(
            text=filtered,
            chart_type=chart_type,
            chart_data=df if not df.empty else None,
            chart_config=chart_config,
            template_used=template_name,
            latency_ms=latency_ms,
            blocked=not is_safe_output,
        )


if __name__ == "__main__":
    print("=" * 55)
    print("SMARD Energy Intelligence Agent — CLI Test Mode")
    print("=" * 55)

    agent = EnergyAgent()
    test_questions = [
        "Hello, what can you help me with?",
        "What is the renewable energy forecast for the next 7 days?",
        "Were there any anomalies in the last 90 days?",
        "What was Germany's renewable share last month?",
        "Which energy source generated the most power recently?",
        "How does renewable generation compare to last year?",
    ]

    for q in test_questions:
        print(f"\nQ: {q}")
        r = agent.ask(q)
        print(f"Template: {r.template_used} | Latency: {r.latency_ms:.0f}ms")
        print(f"Answer: {r.text[:250]}")
        if r.chart_data is not None:
            print(f"Chart: {r.chart_type} ({len(r.chart_data)} rows)")
        print("-" * 40)
