#!/usr/bin/env python3
"""Batch probe Codex Responses WebSocket routing without writing secrets.

Input accepts the NewAPI account export shape (credentials may be nested) or a
small hand-written list. Output is JSON Lines containing only routing and
timing metadata, never response text or raw events. --html-dir explicitly opts
in to saving completed response text locally; treat it as untrusted content.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import ssl
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


def _first(obj: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = obj.get(name)
        if value not in (None, ""):
            return value
    return None


def _flatten(obj: Any, wanted: set[str], out: dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            lower = key.lower()
            if lower in wanted and value not in (None, ""):
                out[lower] = value
            _flatten(value, wanted, out)
    elif isinstance(obj, list):
        for value in obj:
            _flatten(value, wanted, out)


def _load_accounts(path: Path) -> list[dict[str, Any]]:
    if str(path) == "-":
        raw = json.load(sys.stdin)
    else:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    if isinstance(raw, dict):
        raw = raw.get("accounts", raw.get("data", [raw]))
    if not isinstance(raw, list):
        raise ValueError("accounts JSON must be an object or array")
    accounts = []
    for index, item in enumerate(raw, 1):
        if not isinstance(item, dict):
            continue
        credentials = item.get("credentials")
        merged = dict(credentials) if isinstance(credentials, dict) else {}
        merged.update(item)
        token = _first(merged, "access_token", "token", "oauth_token")
        if not token:
            accounts.append({"label": str(_first(merged, "id", "name", "email") or index), "error": "missing_token"})
            continue
        accounts.append({
            "label": str(_first(merged, "id", "name", "email") or index),
            "token": str(token),
            "account_id": str(_first(merged, "account_id", "chatgpt_account_id", "chatgptAccountId") or ""),
            "proxy": str(_first(merged, "proxy_url", "proxy") or ""),
        })
    return accounts


def _proxy_kwargs(proxy: str) -> dict[str, Any]:
    # Select the route explicitly without mutating process-wide environment in
    # worker threads. With no account proxy, '*' tells websocket-client to skip
    # environment proxies; with one, the nonmatching list below prevents bypass.
    if not proxy:
        return {"http_no_proxy": ["*"]}
    parsed = urlparse(proxy)
    # websocket-client's HTTP CONNECT option is not an HTTPS-to-proxy transport.
    # Do not silently downgrade an https:// proxy to plaintext HTTP.
    if parsed.scheme not in {"http", "socks5", "socks5h"} or not parsed.hostname:
        raise ValueError("proxy must be http:// or socks5(h)://")
    kwargs: dict[str, Any] = {
        "http_proxy_host": parsed.hostname,
        "http_proxy_port": parsed.port or (1080 if parsed.scheme.startswith("socks5") else 80),
        "proxy_type": parsed.scheme,
        # A nonempty list suppresses websocket-client's implicit localhost and
        # environment bypasses. An empty hostname cannot match a valid WSS URL.
        "http_no_proxy": [""],
    }
    if parsed.username:
        kwargs["http_proxy_auth"] = (unquote(parsed.username), unquote(parsed.password or ""))
    return kwargs


def _terminal_output_text(response: dict[str, Any]) -> dict[tuple[int, int], list[str]]:
    """Extract only output_text content, never IDs, tool arguments or reasoning."""
    texts: dict[tuple[int, int], list[str]] = {}
    output = response.get("output")
    if not isinstance(output, list):
        return texts
    for output_index, item in enumerate(output):
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for content_index, part in enumerate(content):
            if (isinstance(part, dict) and part.get("type") == "output_text"
                    and isinstance(part.get("text"), str)):
                texts[(output_index, content_index)] = [part["text"]]
    return texts


def _save_html(html_dir: Path, text: str) -> str:
    html_dir.mkdir(parents=True, exist_ok=True)
    # No account-supplied label becomes a path. Exclusive creation also rejects
    # existing files/symlinks, even in the unlikely event of a UUID collision.
    path = html_dir / f"probe-{uuid.uuid4().hex}.html"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(text)
    return path.name


def _engine_family(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False).lower()
    if "gpt56sol-codex" in text:
        return "sol"
    if "gpt56lun-codex" in text:
        return "luna"
    return "unknown"


def _probe(account: dict[str, Any], args: argparse.Namespace, attempt: int) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {
        "label": account["label"],
        "attempt": attempt,
        "started_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "requested_model": args.model,
        "effort": args.effort,
        "status": "error",
    }
    answer_parts: dict[tuple[int, int], list[str]] = {}
    done_parts: set[tuple[int, int]] = set()
    if account.get("proxy"):
        result["proxy_id"] = hashlib.sha256(account["proxy"].encode("utf-8")).hexdigest()[:12]
    if account.get("error"):
        result["error"] = account["error"]
        return result
    try:
        connect_kwargs = _proxy_kwargs(account.get("proxy", ""))
        endpoint = urlparse(args.url)
        if endpoint.scheme != "wss" or not endpoint.hostname or endpoint.username or endpoint.password:
            raise ValueError("endpoint must be WSS without URL credentials")
        import websocket  # type: ignore

        request_id = str(uuid.uuid4())
        turn_metadata = json.dumps(
            {
                "installation_id": request_id,
                "session_id": request_id,
                "thread_id": request_id,
                "window_id": f"{request_id}:0",
                "request_kind": "turn",
            },
            separators=(",", ":"),
        )
        headers = [
            f"Authorization: Bearer {account['token']}",
            "Originator: codex-tui",
            "X-Codex-Beta-Features: remote_compaction_v2",
            "X-Responsesapi-Include-Timing-Metrics: true",
            f"X-Codex-Turn-Metadata: {turn_metadata}",
            f"X-Codex-Window-Id: {request_id}:0",
            f"X-Client-Request-Id: {request_id}",
            f"Session-Id: {request_id}",
            f"Thread-Id: {request_id}",
            "User-Agent: codex-tui/0.153.4",
        ]
        if account.get("account_id"):
            headers.append(f"Chatgpt-Account-Id: {account['account_id']}")
        ws = websocket.create_connection(
            args.url,
            header=headers,
            origin="https://chatgpt.com",
            timeout=args.timeout,
            enable_multithread=True,
            sslopt={"cert_reqs": ssl.CERT_REQUIRED, "check_hostname": True},
            redirect_limit=0,
            **connect_kwargs,
        )
        try:
            payload = {
                "type": "response.create",
                "model": args.model,
                "reasoning": {"effort": args.effort},
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": args.prompt}],
                }],
            }
            ws.send(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            while True:
                raw = ws.recv()
                if raw in (None, "", b""):
                    raise RuntimeError("websocket closed before terminal event")
                try:
                    event = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(event, dict):
                    continue
                event_type = str(event.get("type", ""))
                if event_type:
                    result["last_event_type"] = event_type
                if event_type in {"response.output_text.delta", "response.output_text.done"}:
                    key = (event.get("output_index", 0), event.get("content_index", 0))
                    is_done = event_type == "response.output_text.done"
                    text_value = event.get("text" if is_done else "delta")
                    if (all(isinstance(index, int) and index >= 0 for index in key)
                            and isinstance(text_value, str)):
                        if is_done:
                            answer_parts[key] = [text_value]
                            done_parts.add(key)
                        elif key not in done_parts:
                            answer_parts.setdefault(key, []).append(text_value)
                wanted = {
                    "engine_ids", "x-codex-safety-buffering-enabled",
                    "x-codex-safety-buffering-faster-model", "x-codex-primary-used-percent",
                    "x-codex-primary-window-minutes", "x-codex-active-limit",
                    "limit_reached", "model",
                }
                found: dict[str, Any] = {}
                _flatten(event, wanted, found)
                # Search routing metadata only: scanning the entire event would
                # leak matching substrings of output_text or upstream errors.
                routing_text = json.dumps({key: found[key] for key in (
                    "engine_ids", "model", "x-codex-safety-buffering-faster-model"
                ) if key in found}, ensure_ascii=False)
                abuse_ids = sorted(set(re.findall(
                    r"gpt-5\.6-sol-codex-[A-Za-z0-9_-]*abuse[A-Za-z0-9_-]*",
                    routing_text, re.IGNORECASE,
                )))
                if abuse_ids:
                    result["abuse_model_ids"] = abuse_ids
                    result["abuse_detected"] = True
                if "engine_ids" in found:
                    result["engine_ids"] = found["engine_ids"]
                    result["engine_family"] = _engine_family(found["engine_ids"])
                if isinstance(found.get("model"), (str, int, float)):
                    result["event_model"] = str(found["model"])
                for key in wanted - {"engine_ids", "model"}:
                    if key in found:
                        result[key.replace(".", "_")] = found[key]
                if event_type == "response.completed":
                    response = event.get("response")
                    if isinstance(response, dict):
                        result["response_model"] = response.get("model")
                        # Terminal text replaces the same streamed content part,
                        # and supplies any parts for which no text events arrived.
                        answer_parts.update(_terminal_output_text(response))
                    texts = ["".join(answer_parts[key]) for key in sorted(answer_parts)]
                    # Join chunks before matching, but never join independent
                    # content parts into a fictitious multi-digit answer.
                    nums = sorted({num for text in texts
                                   for num in re.findall(r"(?<![\d.])(\d{1,3})(?![\d.])", text)})
                    if nums:
                        result["answer_candidates"] = nums
                    html_dir = getattr(args, "html_dir", None)
                    if html_dir is not None and any(texts):
                        result["html_file"] = _save_html(html_dir, "\n".join(texts))
                    result["status"] = "completed"
                    break
                if event_type in {"response.failed", "error"}:
                    result["status"] = "failed"
                    result["error_type"] = event_type
                    # Upstream error messages/codes may echo credentials or body
                    # text. Keep only the event classification, not free-form data.
                    break
        finally:
            ws.close()
    except Exception as exc:
        # Even a partially echoed/decoded proxy password or an exception raised
        # while formatting an error must not reach JSONL, stderr or a traceback.
        result["status"] = "error"
        result["error_type"] = type(exc).__name__
        result["error"] = "probe_failed"
    result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 1)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accounts", required=True, type=Path, help="NewAPI export, JSON account list, or - for stdin")
    parser.add_argument("--url", default="wss://chatgpt.com/backend-api/codex/responses")
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--effort", choices=("none", "minimal", "low", "medium", "high", "xhigh", "max"), default="low")
    parser.add_argument("--prompt", default="Reply with exactly OK.")
    parser.add_argument("--prompt-file", type=Path)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path, help="JSONL output path (default: stdout)")
    parser.add_argument("--html-dir", type=Path, help="OPT IN: save untrusted completed response text as unique local HTML files")
    args = parser.parse_args()
    if args.prompt_file:
        args.prompt = args.prompt_file.read_text(encoding="utf-8")
    if args.concurrency < 1 or args.repeat < 1:
        parser.error("--concurrency and --repeat must be positive")
    accounts = _load_accounts(args.accounts)
    jobs = [(account, attempt) for account in accounts for attempt in range(1, args.repeat + 1)]
    output = args.output.open("w", encoding="utf-8") if args.output else None
    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [pool.submit(_probe, account, args, attempt) for account, attempt in jobs]
            for future in as_completed(futures):
                line = json.dumps(future.result(), ensure_ascii=False, separators=(",", ":"))
                print(line, file=output or sys.stdout, flush=True)
    finally:
        if output:
            output.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
