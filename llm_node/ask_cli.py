"""
    python3 -m llm_node.ask_cli "what currencies does AxonFx support?"
    python3 -m llm_node.ask_cli --host 127.0.0.1:7100 "how does a transfer work?"
"""
import argparse
import grpc
from llm_node import llm_pb2, llm_pb2_grpc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1:7100")
    parser.add_argument("text", nargs="+")
    args = parser.parse_args()

    stub = llm_pb2_grpc.LLMServiceStub(grpc.insecure_channel(args.host))
    resp = stub.Ask(llm_pb2.AskRequest(text=" ".join(args.text)))
    tag = "answer" if resp.answered else "unanswered"
    print(f"[{tag}] {resp.reply}")


if __name__ == "__main__":
    main()

