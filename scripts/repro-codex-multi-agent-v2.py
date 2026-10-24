#!/usr/bin/env python3
"""复现 Codex Multi-Agent v2 子任务交付边界。

这是一个本地、无需凭据的协议实验脚本：启动 mock Responses 上游和一个可还原
临时 collaboration namespace 的代理，再用真实 Codex CLI 请求该代理。脚本自身
不读取或输出凭据，模型请求只发往本地 mock；Codex CLI 的 CODEX_HOME 指向本次
运行的临时目录，不会读写调用者日常使用的配置和会话。

示例：
  python scripts/repro-codex-multi-agent-v2.py --codex-exe C:/path/to/codex.exe --child-model <model> --mode compat
  python scripts/repro-codex-multi-agent-v2.py --codex-exe C:/path/to/codex.exe --child-model <model> --mode native

compat：代理把 collaboration-optimize 还原为 collaboration，mock 返回
        encrypted_function_args: []。预期子请求中的 agent_message 只包含明文
        input_text。
native：不做还原，也不返回 encrypted_function_args。预期子请求中的
        agent_message 包含 encrypted_content（即用户报告的失败形态）。
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Windows 控制台默认可能是 GBK；统一按 UTF-8 输出，避免中文提示乱码。
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def sse(events):
    out = b""
    for name, data in events:
        out += f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode()
    return out


def completed(response_id):
    return (
        "response.completed",
        {
            "type": "response.completed",
            "response": {
                "id": response_id,
                "status": "completed",
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        },
    )


class Capture:
    def __init__(self, out_dir):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.count = 0

    def save(self, body):
        with self.lock:
            self.count += 1
            index = self.count
        (self.out_dir / f"request-{index}.json").write_bytes(body)
        return index


def make_mock_handler(capture, mode, child_model):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            index = capture.save(body)
            try:
                data = json.loads(body)
            except Exception:
                data = {}
            input_items = data.get("input") or []
            call_names = [
                item.get("name")
                for item in input_items
                if isinstance(item, dict) and item.get("type") == "function_call"
            ]
            is_child = any(
                isinstance(item, dict) and item.get("type") == "agent_message"
                for item in input_items
            )

            if is_child:
                events = [
                    ("response.created", {"type": "response.created", "response": {"id": f"resp_child_{index}"}}),
                    (
                        "response.output_item.done",
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "message",
                                "id": f"msg_child_{index}",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "REPRO_CHILD_OK result=323"}],
                            },
                        },
                    ),
                    completed(f"resp_child_{index}"),
                ]
            elif "wait_agent" in call_names:
                events = [
                    ("response.created", {"type": "response.created", "response": {"id": f"resp_done_{index}"}}),
                    (
                        "response.output_item.done",
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "message",
                                "id": f"msg_done_{index}",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "REPRO_PARENT_DONE"}],
                            },
                        },
                    ),
                    completed(f"resp_done_{index}"),
                ]
            elif "spawn_agent" in call_names:
                events = [
                    ("response.created", {"type": "response.created", "response": {"id": f"resp_wait_{index}"}}),
                    (
                        "response.output_item.done",
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "function_call",
                                "call_id": f"call_wait_{index}",
                                "namespace": "collaboration",
                                "name": "wait_agent",
                                "arguments": json.dumps({"timeout_ms": 30000}),
                                "encrypted_function_args": [],
                            },
                        },
                    ),
                    completed(f"resp_wait_{index}"),
                ]
            else:
                item = {
                    "type": "function_call",
                    "call_id": f"call_spawn_{index}",
                    "namespace": "collaboration-optimize" if mode == "compat" else "collaboration",
                    "name": "spawn_agent",
                    "arguments": json.dumps(
                        {
                            "task_name": "repro_child",
                            "model": child_model,
                            "reasoning_effort": "low",
                            "fork_turns": "none",
                            "message": "REPRO_TASK_323 compute 17*19 and reply REPRO_CHILD_OK result=323",
                        },
                        ensure_ascii=False,
                    ),
                }
                if mode == "compat":
                    item["encrypted_function_args"] = []
                events = [
                    ("response.created", {"type": "response.created", "response": {"id": f"resp_spawn_{index}"}}),
                    ("response.output_item.done", {"type": "response.output_item.done", "item": item}),
                    completed(f"resp_spawn_{index}"),
                ]

            payload = sse(events)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def make_proxy_handler(mock_port, rewrite):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            conn = HTTPConnection("127.0.0.1", mock_port, timeout=120)
            conn.request("POST", self.path, body=body, headers={"Content-Type": "application/json"})
            response = conn.getresponse()
            payload = response.read()
            if rewrite:
                payload = payload.replace(b'"collaboration-optimize"', b'"collaboration"')
                payload = payload.replace(b"collaboration-optimize.", b"collaboration.")
                payload = payload.replace(b"collaboration-optimize__", b"collaboration__")
            self.send_response(response.status)
            self.send_header("Content-Type", response.getheader("Content-Type", "text/event-stream"))
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return Handler


def start_server(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def find_child_agent_message(request_path, child_model):
    data = json.loads(request_path.read_text(encoding="utf-8"))
    if data.get("model") != child_model:
        return None
    for item in data.get("input") or []:
        if isinstance(item, dict) and item.get("type") == "agent_message":
            return item
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex-exe", required=True, help="官方 Codex CLI 可执行文件路径")
    parser.add_argument("--mode", choices=("compat", "native"), default="compat")
    parser.add_argument("--parent-model", default="gpt-5.5")
    parser.add_argument(
        "--child-model",
        required=True,
        help="spawn_agent 可用的子模型名，由调用者按本机 Codex catalog 选择",
    )
    parser.add_argument("--provider", default="custom")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    work_dir = Path(tempfile.mkdtemp(prefix="codex-mav2-repro-"))
    capture = Capture(work_dir / "requests")
    mock = start_server(make_mock_handler(capture, args.mode, args.child_model))
    proxy = start_server(make_proxy_handler(mock.server_port, args.mode == "compat"))
    workspace = work_dir / "workspace"
    workspace.mkdir()
    # 隔离 Codex 的配置与会话目录，避免读写调用者日常使用的 CODEX_HOME。
    codex_home = work_dir / "codex-home"
    codex_home.mkdir()
    child_env = os.environ.copy()
    child_env["CODEX_HOME"] = str(codex_home)

    prompt = (
        "Spawn exactly one child with task_name repro_child, model "
        f"{args.child_model}, and message REPRO_TASK_323 compute 17*19. "
        "Then wait for it and report its reply."
    )
    command = [
        args.codex_exe,
        "exec",
        "--json",
        "--sandbox",
        "read-only",
        "--skip-git-repo-check",
        "-C",
        str(workspace),
        "-m",
        args.parent_model,
        "-c",
        f'model_provider="{args.provider}"',
        "-c",
        'model_reasoning_effort="low"',
        "-c",
        "features.multi_agent=true",
        "-c",
        "features.multi_agent_v2=true",
        "-c",
        "agents.max_threads=2",
        "-c",
        "agents.max_depth=1",
        "-c",
        f'model_providers.{args.provider}.name="repro"',
        "-c",
        f'model_providers.{args.provider}.base_url="http://127.0.0.1:{proxy.server_port}/v1"',
        "-c",
        f'model_providers.{args.provider}.wire_api="responses"',
        "-c",
        f"model_providers.{args.provider}.requires_openai_auth=false",
        "-c",
        f"model_providers.{args.provider}.request_max_retries=0",
        "-c",
        f"model_providers.{args.provider}.stream_max_retries=0",
        prompt,
    ]

    print(f"工作目录: {work_dir}")
    print("正在用真实 Codex CLI 请求本地 mock 上游...")
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=args.timeout, env=child_env
        )
    except subprocess.TimeoutExpired:
        print("FAIL: Codex CLI 超时")
        return 2
    finally:
        mock.shutdown()
        proxy.shutdown()

    print(f"codex 退出码: {result.returncode}")
    child_item = None
    for request_path in sorted(capture.out_dir.glob("request-*.json")):
        item = find_child_agent_message(request_path, args.child_model)
        if item is not None:
            child_item = (request_path, item)
            break

    if child_item is None:
        print("FAIL: 未捕获到包含 agent_message 的子请求")
        print(result.stdout[-2000:])
        print(result.stderr[-2000:])
        return 2

    request_path, item = child_item
    parts = [
        (part.get("type"), len(part.get("text") or part.get("encrypted_content") or ""))
        for part in item.get("content") or []
        if isinstance(part, dict)
    ]
    print(f"子请求: {request_path.name}")
    print(f"agent_message content 组成: {parts}")

    has_encrypted = any(part_type == "encrypted_content" for part_type, _ in parts)
    has_plaintext = any(part_type == "input_text" for part_type, _ in parts)
    if args.mode == "compat":
        ok = has_plaintext and not has_encrypted
        expectation = "纯明文 input_text"
    else:
        ok = has_encrypted
        expectation = "不透明 encrypted_content"
    print(f"预期: {expectation}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
