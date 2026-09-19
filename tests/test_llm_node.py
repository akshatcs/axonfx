import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from llm_node.server import LLMServiceServicer, _load_faq, _NO_BACKEND_MSG, FAQ_PATH


def _clear_llm_provider():
    """Returns the original LLM_PROVIDER value (or None) so a test can
    restore it afterward - tests must not depend on, or pollute, the
    ambient environment."""
    return os.environ.pop("LLM_PROVIDER", None)


def _restore_llm_provider(original):
    if original is not None:
        os.environ["LLM_PROVIDER"] = original


def test_faq_file_exists_and_has_content():
    # Sanity check: the file the whole node depends on actually exists
    # and isn't empty - if this fails, every question would fail too.
    text = _load_faq()
    assert text, f"expected real content at {FAQ_PATH}"
    assert "AxonFx" in text


def test_empty_question_asks_for_a_question():
    servicer = LLMServiceServicer(faq_text="some faq content", log=lambda msg: None)
    resp = servicer.Ask(_FakeRequest(""), None)
    assert resp.answered is False
    assert "question" in resp.reply.lower()


def test_fails_honestly_with_no_backend_configured():
    # There is deliberately no fallback - if no LLM backend is
    # configured, Ask() must return a clear, actionable error rather
    # than attempting some other kind of answer.
    original = _clear_llm_provider()
    try:
        servicer = LLMServiceServicer(faq_text="some faq content", log=lambda msg: None)
        resp = servicer.Ask(_FakeRequest("what currencies are supported?"), None)
        assert resp.answered is False
        assert resp.reply == _NO_BACKEND_MSG
    finally:
        _restore_llm_provider(original)


def test_fails_honestly_with_missing_faq_content():
    # Backend configured but faq_text is empty (e.g. FAQ.md couldn't be
    # read) - must fail honestly rather than ask the model to answer
    # from nothing.
    os.environ["LLM_PROVIDER"] = "ollama"
    try:
        servicer = LLMServiceServicer(faq_text="", log=lambda msg: None)
        resp = servicer.Ask(_FakeRequest("what currencies are supported?"), None)
        assert resp.answered is False
        assert "FAQ.md" in resp.reply
    finally:
        os.environ.pop("LLM_PROVIDER", None)


class _FakeRequest:
    """Minimal stand-in for the protobuf AskRequest - Ask() only ever
    reads .text off it, so a real gRPC request isn't needed to test
    this in isolation."""
    def __init__(self, text):
        self.text = text


if __name__ == "__main__":
    test_faq_file_exists_and_has_content()
    test_empty_question_asks_for_a_question()
    test_fails_honestly_with_no_backend_configured()
    test_fails_honestly_with_missing_faq_content()
    print("all LLM node tests passed")

