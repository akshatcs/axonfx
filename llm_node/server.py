"""
Node 1: the AI/LLM server. Genuinely independent now - unlike earlier
versions of this project, it holds no connection to the Raft cluster at
all, no state, nothing. It answers general questions about how AxonFx
works, grounded in FAQ.md, using a local LLM (see llm_api.py). It
cannot submit transfers and cannot read live balances or transfer
history - use client/cli.py directly for either of those.

There is deliberately no fallback if the LLM backend isn't configured
or a call fails - that request fails honestly, with a clear explanation
of what to check, rather than silently answering some other way.

    python3 -m llm_node.server
"""

import argparse
import os
import time
from concurrent import futures

import grpc

from llm_node import llm_pb2, llm_pb2_grpc
from llm_node.llm_api import api_available, answer_from_faq

FAQ_PATH = os.path.join(os.path.dirname(__file__), "FAQ.md")

_NO_BACKEND_MSG = (
    "No LLM backend is configured - set LLM_PROVIDER=ollama, make sure "
    "`ollama serve` is running with the model pulled, then restart this node."
)
_BACKEND_FAILED_MSG = (
    "Couldn't reach the LLM backend for this request - check that "
    "`ollama serve` is running and the model is pulled (see llm_api.py), "
    "then try again."
)


def _load_faq():
    try:
        with open(FAQ_PATH, encoding="utf-8") as f:
            return f.read()
    except OSError:
        return ""


class LLMServiceServicer:
    def __init__(self, faq_text, log=print):
        self.faq_text = faq_text
        self.log = log

    def Ask(self, request, context):
        text = request.text.strip()
        if not text:
            return llm_pb2.AskResponse(reply="Ask me a question about AxonFx.", answered=False)

        if not api_available():
            return llm_pb2.AskResponse(reply=_NO_BACKEND_MSG, answered=False)

        if not self.faq_text:
            return llm_pb2.AskResponse(reply=(
                f"FAQ.md is missing or empty (expected at {FAQ_PATH}) - "
                "nothing to answer from."), answered=False)

        reply = answer_from_faq(text, self.faq_text)
        if reply is None:
            self.log("LLM backend unavailable/failed for this request")
            return llm_pb2.AskResponse(reply=_BACKEND_FAILED_MSG, answered=False)
        self.log("answered via LLM backend")
        return llm_pb2.AskResponse(reply=reply, answered=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7100)
    args = parser.parse_args()

    faq_text = _load_faq()

    def log(msg):
        print(f"[llm-node] {msg}", flush=True)

    if not faq_text:
        log(f"WARNING: could not read {FAQ_PATH} - every question will fail until it exists")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(faq_text, log=log), server)
    addr = f"0.0.0.0:{args.port}"
    server.add_insecure_port(addr)
    server.start()
    log(f"listening on {addr} (FAQ-only - no cluster connection)")
    if api_available():
        from llm_node.llm_api import describe_active_provider
        provider, model = describe_active_provider()
        log(f"{provider} backend active - using {model} "
            f"(no fallback - a failed call fails that request honestly)")
    else:
        log("WARNING: no LLM backend configured - every question will fail "
            "until LLM_PROVIDER=ollama is set and this node is restarted "
            "(see llm_api.py for setup)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    main()

