#!/usr/bin/env python3
"""Opt-in real Docker integration test using synthetic SSE and WS only.

Build fixture/main.go as a static Linux binary, then run on a Linux Docker host:
  sudo python3 integration_test.py --fixture /path/to/fixture
Creates uniquely named containers/networks/images, a loopback ephemeral listener,
and private temporary files; removes them in finally. Never reads production
containers or connects to a real database/model. Pull the proxy/base images first.
"""
import argparse
import base64
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import uuid

spec = importlib.util.spec_from_file_location("release", Path(__file__).with_name("release.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT, timeout=90)


def http_get(port, path="/"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        assert response.status == 200, response.status
        return response.read().decode()
    finally:
        conn.close()


def websocket(port):
    conn = socket.create_connection(("127.0.0.1", port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    conn.sendall((f"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
    data = b""
    while not data.endswith(b"\r\n\r\n"):
        chunk = conn.recv(1)
        assert chunk, "WebSocket closed before handshake"
        data += chunk
    assert b" 101 " in data, data
    expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
    assert expected.lower() in data.lower(), data
    return conn


def receive(conn, length):
    chunks = b""
    while len(chunks) < length:
        chunk = conn.recv(length - len(chunks))
        assert chunk, "Connection interrupted"
        chunks += chunk
    return chunks


def echo(conn, text):
    payload = text.encode()
    assert len(payload) < 126
    mask = os.urandom(4)
    conn.sendall(bytes((0x81, 0x80 | len(payload))) + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(payload)))
    head = receive(conn, 2)
    assert head[0] == 0x81 and head[1] < 126, head
    return receive(conn, head[1]).decode()


def sse_next(response, name):
    for _ in range(1000):
        line = response.readline()
        assert line, "SSE interrupted"
        if line.startswith(b"data:"):
            assert line.startswith(f"data: {name}-".encode()), line
            return line
    raise AssertionError("Missing SSE data")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base-image", default="redis:7-alpine")
    parser.add_argument("--proxy-image", default="haproxy:3.2-alpine")
    args = parser.parse_args()
    name = "rolltest-" + uuid.uuid4().hex[:10]
    source = name + "-source"
    network = name + "-data"
    images, source_volumes, connections = [], [], []
    results = []
    with tempfile.TemporaryDirectory(prefix=name + "-") as temporary:
        root = Path(temporary)
        app = None
        try:
            docker("network", "create", network)
            context = root / "build"
            context.mkdir()
            shutil.copyfile(args.fixture, context / "fixture")
            (context / "fixture").chmod(0o755)
            (context / "Dockerfile").write_text(
                f"FROM {args.base_image}\nCOPY fixture /fixture\nCOPY fixture-name /fixture-name\n"
                'ENV DATABASE_HOST=synthetic-postgres REDIS_ADDR=synthetic-redis:6379 CODEX_PORT=8080 IMAGE_JOB_WORKERS=0\n'
                'ENTRYPOINT ["/fixture"]\nCMD []\n', encoding="utf-8")
            for slot in release.SLOTS:
                image = name + ":" + slot
                images.append(image)
                (context / "fixture-name").write_text(slot, encoding="utf-8")
                docker("build", "--pull=false", "-q", "-t", image, str(context))
            docker("run", "-d", "--name", source, "--network", network,
                   "-e", "SYNTHETIC_LITERAL=one$two${THREE}$$four", images[0])
            source_info = json.loads(docker("inspect", source))[0]
            source_volumes = [m["Name"] for m in source_info["Mounts"] if m["Type"] == "volume"]
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                port = available.getsockname()[1]
            state = root / "state"
            state.mkdir(mode=0o700)
            with release.locked(state):
                app = release.Release(state)
                app.prepare(source, name, "127.0.0.1", port, args.proxy_image)
                app.bootstrap(images[0], False, 45)
                assert http_get(port) == "blue"
                env = json.loads(docker("inspect", name + "-blue"))[0]["Config"]["Env"]
                assert "SYNTHETIC_LITERAL=one$two${THREE}$$four" in env, "Compose corrupted literal dollar characters"
                results.append("startup/shared-network/environment-literals")
                conn = websocket(port)
                connections.append(conn)
                assert echo(conn, "before") == "blue:before"
                stream = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                connections.append(stream)
                stream.request("GET", "/sse")
                response = stream.getresponse()
                connections.append(response)
                assert response.status == 200
                sse_next(response, "blue")
                assert app.deploy(images[1], False, 45, 0) is False, "Must retain busy old version"
                assert app.state["active"] == "green"
                assert app.stats()["blue"]["sessions"] >= 2
                assert http_get(port) == "green"
                assert echo(conn, "after-switch") == "blue:after-switch"
                sse_next(response, "blue")
                results.append("SSE-and-WS-survive-switch/new-requests-reach-new-version")
                try:
                    app.deploy(images[0], False, 0, 0)
                    raise AssertionError("Recreated slot with live connections")
                except release.ReleaseError:
                    pass
                results.append("busy-slot-cannot-be-overwritten")
                new_conn = websocket(port)
                connections.append(new_conn)
                assert echo(new_conn, "before-rollback") == "green:before-rollback"
                assert app.rollback(45, 0) is False
                assert http_get(port) == "blue"
                assert echo(conn, "old") == "blue:old"
                assert echo(new_conn, "retained") == "green:retained"
                results.append("rollback-preserves-both-existing-WS-connections")
                new_conn.close()
                assert app.drain(10) is True
                assert not json.loads(docker("inspect", name + "-green"))[0]["State"]["Running"]
                results.append("old-container-stopped-only-after-zero-connections")
                response.close()
                stream.close()
                conn.close()
                assert app.deploy(images[1], False, 45, 10) is True
                assert http_get(port) == "green"
                assert not json.loads(docker("inspect", name + "-blue"))[0]["State"]["Running"]
                # Restart only the test proxy: startup configuration must agree
                # with the runtime switch, not revert to the original backend.
                docker("restart", name + "-edge")
                app.wait_proxy("green", 30)
                assert http_get(port) == "green"
                results.append("subsequent-release-and-proxy-restart-keep-selected-version")
            print(json.dumps({"passed": results, "production_requests": 0}, indent=2), flush=True)
        except BaseException:
            for slot in ("edge", "blue", "green"):
                try:
                    print("SYNTHETIC_LOG", slot, docker("logs", "--tail", "30", name + "-" + slot), flush=True)
                except subprocess.SubprocessError:
                    pass
            raise
        finally:
            for conn in connections:
                conn.close()
            for container in (name + "-edge", name + "-blue", name + "-green", source):
                subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for net in (name + "-ingress", network):
                subprocess.run(["docker", "network", "rm", net], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for volume in source_volumes:
                subprocess.run(["docker", "volume", "rm", volume], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for image in images:
                subprocess.run(["docker", "image", "rm", image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
