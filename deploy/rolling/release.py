#!/usr/bin/env python3
"""Blue/green application releases behind a stable HAProxy listener (Linux).

Only Docker CLI and Python's standard library are required. PostgreSQL/Redis
remain in their original Compose project. Generated files contain credentials;
keep the state directory private and outside the source checkout.
"""
import argparse
import copy
import csv
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager

SLOTS = ("blue", "green")
BACKEND = "app"


class ReleaseError(Exception):
    pass


def atomic_write(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(path, value):
    atomic_write(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def compose_literals(value):
    """Compose interpolates even JSON environment values; protect literal '$'."""
    if isinstance(value, str):
        return value.replace("$", "$$")
    if isinstance(value, dict):
        return {key: compose_literals(item) for key, item in value.items()}
    if isinstance(value, list):
        return [compose_literals(item) for item in value]
    return value


def run(arguments, timeout=60):
    try:
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        raise ReleaseError("Docker command unavailable or timed out; containers have been retained.") from None
    if result.returncode:
        # Docker/Compose diagnostics can include interpolated credentials. Never
        # echo arbitrary stderr, argv or the rendered application environment.
        raise ReleaseError("Docker command failed; inspect Docker locally. No database service was stopped.")
    return result.stdout


def inspect_container(name):
    return json.loads(run(["docker", "inspect", "--type", "container", name]))[0]


def snapshot(source, project, bind, port, proxy_image):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", project):
        raise ReleaseError("Project must match [a-z][a-z0-9-]{0,39}.")
    try:
        ipaddress.IPv4Address(bind)
    except ValueError:
        raise ReleaseError("Bind must be an IPv4 address.") from None
    if not 1 <= port <= 65535:
        raise ReleaseError("Listener port must be 1..65535.")
    config, host = source["Config"], source["HostConfig"]
    env = dict(item.split("=", 1) for item in config.get("Env", []) if "=" in item)
    if env.get("DATABASE_DRIVER", "postgres").lower() not in ("", "postgres"):
        raise ReleaseError("Rolling deployment requires PostgreSQL, not a shared SQLite file.")
    if env.get("CACHE_DRIVER", "redis").lower() not in ("", "redis"):
        raise ReleaseError("Rolling deployment requires shared Redis.")
    if not env.get("DATABASE_HOST") or not env.get("REDIS_ADDR"):
        raise ReleaseError("DATABASE_HOST and REDIS_ADDR must be present in the source container environment.")
    if env.get("IMAGE_JOB_WORKERS", "0").strip() not in ("", "0"):
        raise ReleaseError("This text/WS release workflow does not drain background image workers.")
    if host.get("NetworkMode") in ("host", "none") or str(host.get("NetworkMode", "")).startswith("container:"):
        raise ReleaseError("Source must use ordinary Docker bridge networks.")
    if host.get("Privileged") or host.get("Devices") or host.get("DeviceRequests"):
        raise ReleaseError("Privileged/device container configuration needs a dedicated deployment definition.")
    if host.get("Tmpfs") or config.get("Volumes"):
        # Named/bind volumes are reconstructed below. Reject unmounted image
        # volumes rather than silently create independent storage per version.
        mounted = {item["Destination"] for item in source.get("Mounts", [])}
        if host.get("Tmpfs") or any(path not in mounted for path in (config.get("Volumes") or {})):
            raise ReleaseError("Unmanaged temporary/anonymous storage needs an explicit deployment definition.")
    app_port = int(env.get("CODEX_PORT") or env.get("PORT") or "8080")
    if not 1 <= app_port <= 65535:
        raise ReleaseError("Invalid CODEX_PORT in source.")
    if env.get("CODEX_BIND", "0.0.0.0") not in ("", "0.0.0.0", "::"):
        raise ReleaseError("Application must listen on its container network, not loopback.")
    env["CODEX_BIND"] = "0.0.0.0"
    external = {}
    networks = {"ingress": {"name": project + "-ingress"}}
    for index, name in enumerate(source["NetworkSettings"]["Networks"]):
        key = "data" + str(index)
        external[key] = None
        networks[key] = {"external": True, "name": name}
    if not external:
        raise ReleaseError("Source has no shared Docker network.")
    mounts, volumes = [], {}
    for index, mount in enumerate(source.get("Mounts", [])):
        item = {"type": mount["Type"], "target": mount["Destination"], "read_only": not mount["RW"]}
        if mount["Type"] == "bind":
            item["source"] = mount["Source"]
            item["bind"] = {"create_host_path": False}
            if mount.get("Propagation") not in (None, "", "rprivate"):
                item["bind"]["propagation"] = mount["Propagation"]
        elif mount["Type"] == "volume":
            key = "shared" + str(index)
            volumes[key] = {"external": True, "name": mount["Name"]}
            item["source"] = key
        else:
            raise ReleaseError("Unsupported source mount type; no deployment files were generated.")
        mounts.append(item)
    base = {
        "environment": env, "volumes": mounts,
        "networks": list(external) + ["ingress"], "restart": "unless-stopped",
        "stop_grace_period": "120s",
        "healthcheck": {"test": ["CMD", "wget", "-q", "-O", "/dev/null", f"http://127.0.0.1:{app_port}/health"],
                        "interval": "5s", "timeout": "3s", "retries": 6, "start_period": "60s"},
        "logging": {"driver": "json-file", "options": {"max-size": "100m", "max-file": "3"}},
    }
    for source_key, target in (("User", "user"), ("WorkingDir", "working_dir"), ("Entrypoint", "entrypoint"), ("Cmd", "command")):
        if config.get(source_key):
            base[target] = config[source_key]
    for source_key, target in (("Memory", "mem_limit"), ("MemorySwap", "memswap_limit"), ("ShmSize", "shm_size"),
                               ("PidsLimit", "pids_limit"), ("ReadonlyRootfs", "read_only"), ("Init", "init"),
                               ("Dns", "dns"), ("DnsSearch", "dns_search"), ("ExtraHosts", "extra_hosts"),
                               ("CapAdd", "cap_add"), ("CapDrop", "cap_drop"), ("SecurityOpt", "security_opt")):
        if host.get(source_key):
            base[target] = host[source_key]
    if host.get("NanoCpus"):
        base["cpus"] = host["NanoCpus"] / 1_000_000_000
    if host.get("Ulimits"):
        base["ulimits"] = {u["Name"]: {"soft": u["Soft"], "hard": u["Hard"]} for u in host["Ulimits"]}
    return {"version": 1, "project": project, "source_container": source["Name"].lstrip("/"),
            "bind": bind, "port": port, "app_port": app_port, "proxy_image": proxy_image,
            "base": base, "networks": networks, "volumes": volumes, "active": "blue",
            "slots": {slot: {"image": source["Image"], "reference": config["Image"]} for slot in SLOTS},
            "previous_release": {"slot": "green", "release": {"image": source["Image"], "reference": config["Image"]}},
            "bootstrapped": False, "pending": None}


def compose_config(state, root):
    services = {}
    for slot in SLOTS:
        service = copy.deepcopy(state["base"])
        service.update(image=state["slots"][slot]["image"], container_name=state["project"] + "-" + slot)
        # No host ports on applications: every client must pass the drain counter.
        services[slot] = service
    services["edge"] = {
        "image": state["proxy_image"], "container_name": state["project"] + "-edge",
        "user": "0:0", "restart": "unless-stopped", "networks": ["ingress"],
        "ports": [{"target": 8080, "published": str(state["port"]), "host_ip": state["bind"], "protocol": "tcp"}],
        "volumes": [{"type": "bind", "source": str(root / "proxy"), "target": "/usr/local/etc/haproxy", "read_only": True},
                    {"type": "bind", "source": str(root / "run"), "target": "/run/haproxy"}],
        "logging": {"driver": "json-file", "options": {"max-size": "20m", "max-file": "3"}},
    }
    return compose_literals({"name": state["project"], "services": services,
                             "networks": state["networks"], "volumes": state["volumes"]})


def haproxy_config(state, active=None):
    active = active or state["active"]
    lines = ["global", "    maxconn 20000", "    stats socket /run/haproxy/admin.sock mode 600 level admin",
             "defaults", "    mode http", "    timeout connect 5s", "    timeout client 1h",
             "    timeout server 1h", "    timeout tunnel 24h", "    timeout http-request 60s",
             "    timeout http-keep-alive 60s", "    retries 0", "    option http-server-close",
             "resolvers docker", "    nameserver dns 127.0.0.11:53", "    resolve_retries 3",
             "    timeout resolve 1s", "    timeout retry 1s", "    hold valid 5s", "    hold nx 1s",
             "frontend public", "    bind :8080", "    default_backend app",
             "    option forwardfor", "backend app", "    option httpchk GET /health",
             "    http-check expect status 200"]
    for slot in SLOTS:
        disabled = "" if slot == active else " disabled"
        lines.append(f"    server {slot} {state['project']}-{slot}:{state['app_port']} check inter 2s rise 2 fall 3 resolvers docker init-addr libc,none{disabled}")
    return "\n".join(lines) + "\n"


def parse_stats(text):
    if not text.startswith("# "):
        raise ReleaseError("Invalid HAProxy statistics; refusing to stop any application.")
    result = {}
    for row in csv.DictReader(io.StringIO(text[2:])):
        if row.get("pxname") == BACKEND and row.get("svname") in SLOTS:
            if row["svname"] in result:
                raise ReleaseError("Duplicate HAProxy server counters; refusing to stop any application.")
            try:
                status, sessions, queued = row["status"], int(row["scur"]), int(row["qcur"])
                if not status or sessions < 0 or queued < 0:
                    raise ValueError("invalid server counters")
                result[row["svname"]] = {"status": status, "sessions": sessions, "queued": queued}
            except (KeyError, ValueError, TypeError):
                raise ReleaseError("Incomplete HAProxy counters; refusing to stop any application.") from None
    if set(result) != set(SLOTS):
        raise ReleaseError("Missing HAProxy servers; refusing to stop any application.")
    return result


class Release:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.path = self.root / "state.json"
        self.state = json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else None

    def persist(self):
        save_json(self.path, self.state)

    def render(self):
        for folder in (self.root / "proxy", self.root / "run"):
            folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        save_json(self.root / "compose.json", compose_config(self.state, self.root))
        atomic_write(self.root / "proxy" / "haproxy.cfg", haproxy_config(self.state))

    def compose(self, *args, timeout=180):
        return run(["docker", "compose", "--project-name", self.state["project"], "-f", str(self.root / "compose.json"), *args], timeout)

    def socket_command(self, command):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(5)
                client.connect(str(self.root / "run" / "admin.sock"))
                client.sendall((command + "\n").encode())
                client.shutdown(socket.SHUT_WR)
                chunks = []
                size = 0
                while True:
                    block = client.recv(65536)
                    if not block:
                        break
                    size += len(block)
                    if size > 2_000_000:
                        raise ReleaseError("HAProxy reply exceeded its bound.")
                    chunks.append(block)
            return b"".join(chunks).decode("utf-8", "replace")
        except OSError:
            raise ReleaseError("HAProxy control socket unavailable; applications have been retained.") from None

    def stats(self):
        return parse_stats(self.socket_command("show stat"))

    def set_server(self, slot, mode):
        reply = self.socket_command(f"set server {BACKEND}/{slot} state {mode}")
        if reply.strip():
            raise ReleaseError("HAProxy rejected the switch; applications have been retained.")

    def require_initialized(self):
        if not self.state:
            raise ReleaseError("Run prepare first.")

    def require_idle_transaction(self):
        self.require_initialized()
        if self.state.get("pending"):
            raise ReleaseError("An interrupted switch exists. Run recover before another release.")

    def resolve_image(self, reference, pull):
        if not reference or reference.startswith("-") or any(ch.isspace() for ch in reference):
            raise ReleaseError("Invalid image reference.")
        if pull:
            run(["docker", "pull", reference], timeout=600)
        data = json.loads(run(["docker", "image", "inspect", reference]))[0]
        return {"reference": reference, "image": data["Id"]}

    def healthy(self, slot):
        name = self.state["project"] + "-" + slot
        try:
            item = inspect_container(name)
        except ReleaseError:
            return False
        return (item.get("Image") == self.state["slots"][slot]["image"]
                and item["State"].get("Running", False)
                and item["State"].get("Health", {}).get("Status") == "healthy")

    def wait_healthy(self, slot, timeout):
        deadline = time.monotonic() + timeout
        while not self.healthy(slot):
            if time.monotonic() >= deadline:
                raise ReleaseError(f"{slot} did not become healthy; existing traffic has not been switched.")
            time.sleep(2)

    def wait_proxy(self, slot, timeout):
        deadline = time.monotonic() + timeout
        while True:
            try:
                if self.stats()[slot]["status"] == "UP":
                    return
            except ReleaseError:
                pass
            if time.monotonic() >= deadline:
                raise ReleaseError("Proxy readiness timed out; run status/recover before retrying.")
            time.sleep(1)

    def prepare(self, source, project, bind, port, proxy_image):
        if self.state:
            raise ReleaseError("State already exists; prepare will not overwrite credentials or release history.")
        self.state = snapshot(inspect_container(source), project, bind, port, proxy_image)
        self.persist()
        self.render()
        self.compose("config", "--quiet")
        print("Prepared private Compose files. No containers or database services were changed.")

    def bootstrap(self, image, pull, timeout):
        self.require_idle_transaction()
        if self.state["bootstrapped"]:
            raise ReleaseError("Already bootstrapped; use deploy.")
        pinned = self.state.get("bootstrap_image")
        if pinned:
            if image not in (pinned["reference"], pinned["image"]):
                raise ReleaseError("Bootstrap already started. Resume its saved image first, then use deploy for another version.")
        else:
            pinned = self.resolve_image(image, pull)
            self.state["bootstrap_image"] = copy.deepcopy(pinned)
        self.state["slots"]["blue"] = copy.deepcopy(pinned)
        self.persist()
        self.render()
        # A prior invocation may already have a serving listener. Never
        # recreate either container during bootstrap recovery, even when a tag
        # now resolves to a different image. Resume the journaled image only.
        self.compose("up", "-d", "--no-deps", "--no-recreate", "blue")
        self.wait_healthy("blue", timeout)
        self.compose("up", "-d", "--no-deps", "--no-recreate", "edge")
        self.wait_proxy("blue", timeout)
        self.state["bootstrapped"] = True
        self.persist()
        print(f"Listener ready at {self.state['bind']}:{self.state['port']}. The original container is still running.")

    def inactive(self):
        return "green" if self.state["active"] == "blue" else "blue"

    def safe_to_replace(self, slot):
        row = self.stats()[slot]
        if row["sessions"] or row["queued"] or not row["status"].startswith(("DRAIN", "MAINT")):
            raise ReleaseError(f"{slot} still has connections or accepts traffic; run drain first.")

    def deploy(self, image, pull, timeout, drain_timeout):
        self.require_idle_transaction()
        if not self.state["bootstrapped"]:
            raise ReleaseError("Run bootstrap first.")
        target = self.inactive()
        self.safe_to_replace(target)
        self.state["slots"][target] = self.resolve_image(image, pull)
        self.persist()
        self.render()
        self.compose("up", "-d", "--no-deps", target)
        self.wait_healthy(target, timeout)
        self.switch(target, timeout)
        return self.drain(drain_timeout)

    def switch(self, target, timeout):
        previous = self.state["active"]
        self.state["pending"] = {"target": target, "previous": previous}
        self.persist()
        # Persist the desired startup routing before changing live routing. A
        # crash is journaled; recover verifies health and completes this switch.
        atomic_write(self.root / "proxy" / "haproxy.cfg", haproxy_config(self.state, target))
        self.set_server(target, "ready")
        self.wait_proxy(target, timeout)
        self.set_server(previous, "drain")
        stats = self.stats()
        if stats[target]["status"] != "UP" or not stats[previous]["status"].startswith(("DRAIN", "MAINT")):
            raise ReleaseError("Switch not confirmed. Both applications retained; run recover.")
        self.state["previous_release"] = {"slot": previous, "release": copy.deepcopy(self.state["slots"][previous])}
        self.state["active"] = target
        self.state["pending"] = None
        self.persist()
        print(f"New requests now use {target}; existing connections stay on {previous}.")

    def recover(self, timeout, abort=False):
        self.require_initialized()
        pending = self.state.get("pending")
        if not pending:
            raise ReleaseError("No interrupted switch to recover.")
        target = pending["target"]
        if abort:
            previous = pending["previous"]
            self.wait_healthy(previous, timeout)
            atomic_write(self.root / "proxy" / "haproxy.cfg", haproxy_config(self.state, previous))
            self.set_server(previous, "ready")
            self.wait_proxy(previous, timeout)
            self.set_server(target, "drain")
            self.state["active"] = previous
            self.state["pending"] = None
            self.persist()
            print("Previous routing restored. Candidate connections are retained; run drain later.")
        else:
            self.wait_healthy(target, timeout)
            self.switch(target, timeout)

    def rollback(self, timeout, drain_timeout):
        self.require_idle_transaction()
        if not self.state["bootstrapped"]:
            raise ReleaseError("Run bootstrap first.")
        target = self.inactive()
        previous = self.state.get("previous_release", {})
        restore_image = previous.get("slot") == target and previous.get("release") != self.state["slots"][target]
        # A failed candidate may already have replaced the inactive slot. Keep
        # the last successful image separately, so rollback never activates an
        # unverified candidate just because it eventually becomes healthy.
        if restore_image or not self.healthy(target):
            self.safe_to_replace(target)
            if restore_image:
                self.state["slots"][target] = copy.deepcopy(previous["release"])
                self.persist()
            # Also render when recovering after a crash between metadata
            # persistence and compose generation. healthy() verifies image ID.
            self.render()
            self.compose("up", "-d", "--no-deps", target)
            self.wait_healthy(target, timeout)
        self.switch(target, timeout)
        return self.drain(drain_timeout)

    def drain(self, timeout):
        self.require_idle_transaction()
        if not self.state["bootstrapped"]:
            raise ReleaseError("Run bootstrap first.")
        old = self.inactive()
        deadline = time.monotonic() + timeout
        while True:
            stats = self.stats()
            row = stats[old]
            if stats[self.state["active"]]["status"] != "UP":
                raise ReleaseError("Active application is not UP; retaining the previous application for rollback.")
            if not row["status"].startswith(("DRAIN", "MAINT")):
                raise ReleaseError("Old application is not draining; refusing to stop it.")
            if row["sessions"] == 0 and row["queued"] == 0:
                self.compose("stop", "--timeout", "120", old)
                print(f"{old} drained and stopped. Its image remains available for rollback.")
                return True
            if time.monotonic() >= deadline:
                print(f"Drain pending: {old} has {row['sessions']} connections and {row['queued']} queued. It remains running. Run drain later.")
                return False
            time.sleep(min(2, max(0, deadline - time.monotonic())))

    def status(self):
        self.require_initialized()
        result = {key: self.state.get(key) for key in ("project", "bind", "port", "active", "bootstrapped", "bootstrap_image", "pending", "slots", "previous_release")}
        if self.state["bootstrapped"]:
            result["proxy"] = self.stats()
        print(json.dumps(result, ensure_ascii=False, indent=2))


@contextmanager
def locked(root):
    if sys.platform != "linux":
        raise ReleaseError("Deployment commands require a Linux Docker host.")
    import fcntl
    root = Path(root).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_mode & 0o077:
        raise ReleaseError("State directory must have permissions 700 (contains credentials).")
    with open(root / ".lock", "a", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ReleaseError("Another release command is running.") from None
        yield


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--state-dir", default="/var/lib/codex2api-rollout")
    sub = result.add_subparsers(dest="action", required=True)
    prepare = sub.add_parser("prepare", help="Snapshot application configuration; does not start or stop containers")
    prepare.add_argument("--source", default="codex2api")
    prepare.add_argument("--project", default="codex2api-rollout")
    prepare.add_argument("--bind", default="127.0.0.1")
    prepare.add_argument("--port", type=int, default=18910)
    prepare.add_argument("--proxy-image", default="haproxy:3.2-alpine")
    for action in ("bootstrap", "deploy"):
        command = sub.add_parser(action)
        command.add_argument("image")
        command.add_argument("--pull", action="store_true", help="Pull image before pinning its local immutable ID")
        command.add_argument("--ready-timeout", type=int, default=180)
        if action == "deploy":
            command.add_argument("--drain-timeout", type=int, default=300)
    for action in ("rollback", "recover"):
        command = sub.add_parser(action)
        command.add_argument("--ready-timeout", type=int, default=180)
        if action == "rollback":
            command.add_argument("--drain-timeout", type=int, default=300)
        else:
            command.add_argument("--abort", action="store_true", help="Restore previous routing instead of completing the switch")
    sub.add_parser("status")
    drain = sub.add_parser("drain")
    drain.add_argument("--drain-timeout", type=int, default=300)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    for key in ("ready_timeout", "drain_timeout"):
        if getattr(args, key, 0) < 0:
            raise ReleaseError("Timeouts cannot be negative.")
    with locked(args.state_dir):
        release = Release(args.state_dir)
        if args.action == "prepare":
            release.prepare(args.source, args.project, args.bind, args.port, args.proxy_image)
        elif args.action == "bootstrap":
            release.bootstrap(args.image, args.pull, args.ready_timeout)
        elif args.action == "deploy":
            return 0 if release.deploy(args.image, args.pull, args.ready_timeout, args.drain_timeout) else 2
        elif args.action == "rollback":
            return 0 if release.rollback(args.ready_timeout, args.drain_timeout) else 2
        elif args.action == "recover":
            release.recover(args.ready_timeout, args.abort)
        elif args.action == "drain":
            return 0 if release.drain(args.drain_timeout) else 2
        else:
            release.status()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ReleaseError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, KeyError, TypeError):
        print("Error: invalid state, container metadata or local file access. No credentials are printed; inspect locally.", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted; containers retained. Check status/recover before the next release.", file=sys.stderr)
        sys.exit(130)
