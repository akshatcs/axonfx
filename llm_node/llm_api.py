"""
Local-LLM-backed FAQ answering for the LLM node, via Ollama.

Node 1 no longer submits transfers or reads live cluster data - it only
answers general questions about AxonFx, grounded in FAQ.md, using a
local model. If LLM_PROVIDER=ollama isn't set, or the call fails for
any reason (Ollama not running, model not pulled, timeout, an empty
reply), answer_from_faq() returns None - the caller (llm_node/server.py)
turns that into an honest "couldn't answer" response. There is no
fallback to a weaker answering method; a failed call fails that request
outright, rather than degrading silently.

Uses only the Python standard library (urllib) - no new dependency, no
change to requirements.txt.

--- Setup ---

Off by default - nothing here is used unless explicitly turned on:

    export LLM_PROVIDER=ollama

Running Ollama locally (including inside WSL) - a real, small,
pretrained model, not a from-scratch toy:

    curl -fsSL https://ollama.com/install.sh | sh
    ollama serve &                    # if not already running as a service
    ollama pull qwen2.5:1.5b          # default model this file expects

Note for WSL specifically: `ollama serve` needs to actually be running
before this will work - the install script sets it up as a systemd
service on distros with systemd enabled, but older/default WSL setups
often don't have systemd on, in which case run `ollama serve` yourself
in its own terminal and leave it running.

Optionally override the model, timeout, or temperature:

    export OLLAMA_MODEL=qwen2.5:1.5b          # default shown
    export OLLAMA_TIMEOUT_SECONDS=30          # default shown
    export OLLAMA_TEMPERATURE=0.1             # default shown - low on purpose,
                                               # answering from a fixed FAQ should
                                               # be consistent, not "creative"

If calls keep failing, check the node's own log line
("LLM backend unavailable/failed") - the most common cause is
`ollama serve` not actually running, or the model not pulled yet.
"""

import json
import os
import urllib.error
import urllib.request

# Ollama's OpenAI-compatible endpoint - local, no auth. Default port is
# Ollama's own standard, unrelated to any port used elsewhere in AxonFx.
OLLAMA_API_URL = "http://localhost:11434/v1/chat/completions"
DEFAULT_OLLAMA_MODEL = "qwen2.5:1.5b"
# CPU generation is genuinely slower than a hosted API call - a short
# timeout here would fail a request that was actually still working,
# not stuck. Overridable if your machine needs more (or less) room.
OLLAMA_TIMEOUT = float(os.environ.get("OLLAMA_TIMEOUT_SECONDS", "30"))
# Ollama's own default temperature (~0.7-0.8) is tuned for open-ended
# conversation. Answering consistently from a fixed FAQ does better,
# and refuses valid, clearly-covered questions less often, closer to
# deterministic.
OLLAMA_TEMPERATURE = float(os.environ.get("OLLAMA_TEMPERATURE", "0.1"))


def _active_provider():
    """Returns "ollama" if explicitly enabled, else None. Off by
    default - a local, unauthenticated service has no natural "key
    present" signal to auto-detect from, so this has to be opt-in."""
    if os.environ.get("LLM_PROVIDER", "").strip().lower() == "ollama":
        return "ollama"
    return None


def describe_active_provider():
    """(provider_name, model_name_actually_in_use), or (None, None) if
    not enabled. Used only for the node's startup log line."""
    if _active_provider() != "ollama":
        return None, None
    return "ollama", os.environ.get("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)


def api_available():
    return _active_provider() is not None


def _call_ollama(system, user_text, max_tokens):
    body = json.dumps({
        "model": os.environ.get("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL),
        "max_tokens": max_tokens,
        "temperature": OLLAMA_TEMPERATURE,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_text},
        ],
        "stream": False,  # required - our parsing below expects one complete JSON reply, not a stream
    }).encode("utf-8")
    req = urllib.request.Request(
        OLLAMA_API_URL,
        data=body,
        method="POST",
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return payload["choices"][0]["message"]["content"]
    except Exception:
        return None


def answer_from_faq(question, faq_text):
    """Returns a short natural-language answer grounded in faq_text, or
    None on any failure - not configured, Ollama unreachable, timeout,
    or an empty reply all count as failure the same way."""
    if _active_provider() != "ollama":
        return None
    system = (
        "You are a support assistant for AxonFx. Answer the user's question "
        "using ONLY the FAQ document below - never invent facts, numbers, or "
        "behavior that isn't stated in it. If the question isn't covered by "
        "the FAQ, say so honestly in one short sentence rather than guessing. "
        "Keep answers to 2-4 short sentences.\n\n--- FAQ ---\n" + faq_text
    )
    raw = _call_ollama(system, question, max_tokens=300)
    if raw is None or not raw.strip():
        return None
    return raw.strip()

