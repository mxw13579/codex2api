"""Isolated safety tests for release.py; all Docker/socket data is synthetic.

Run with: python -m unittest discover -s deploy/rolling -p 'test_release.py' -v
Only temporary files are used. These tests do not validate real Docker/HAProxy,
SSE/WS traffic, background jobs, or the existence of source bind mounts.
Safety regressions are ordinary failures, not expectedFailure/skip markers.
"""
import copy
from contextlib import nullcontext
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


# Load only the explicitly permitted sibling, without package imports or CLI work.
_SPEC = importlib.util.spec_from_file_location(
    "rolling_release_under_test", Path(__file__).with_name("release.py")
)
release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)

PROJECT = "synthetic-rollout"
SOURCE_IMAGE = "sha256:" + "1" * 64
CANDIDATE_IMAGE = "sha256:" + "2" * 64
CANDIDATE_REFERENCE = "synthetic.invalid/app:candidate"
PROXY_IMAGE = "synthetic.invalid/haproxy@sha256:" + "3" * 64


def source_inspect():
    """Docker-shaped fixture; paths, addresses and values are all invented."""
    return {
        "Name": "/synthetic-source",
        "Image": SOURCE_IMAGE,
        "Config": {
            "Image": "synthetic.invalid/app:mutable",
            "Env": [
                "DATABASE_DRIVER=postgres", "DATABASE_HOST=synthetic-postgres",
                "CACHE_DRIVER=redis", "REDIS_ADDR=synthetic-redis:6379",
                "CODEX_PORT=8187", "CODEX_BIND=::", "IMAGE_JOB_WORKERS=0",
                "SYNTHETIC_LITERAL=one$two${THREE}$$four=value", "EMPTY=",
            ],
            "Volumes": {"/data": {}, "/app/logs": {}},
            "User": "1234:1234", "WorkingDir": "/app",
            "Entrypoint": ["/app/server"], "Cmd": ["--label=synthetic$label"],
        },
        "HostConfig": {
            "NetworkMode": "synthetic-data",
            "PortBindings": {"8187/tcp": [{"HostIp": "0.0.0.0", "HostPort": "18187"}]},
        },
        "NetworkSettings": {
            "Networks": {"synthetic-data": {}, "synthetic-extra": {}},
        },
        "Mounts": [
            {"Type": "volume", "Name": "synthetic-assets", "Source": "/synthetic/volume",
             "Destination": "/data", "RW": True},
            {"Type": "bind", "Source": "/synthetic/logs$archive",
             "Destination": "/app/logs", "RW": False, "Propagation": "rslave"},
        ],
        "State": {"Running": True, "Health": {"Status": "healthy"}},
    }


def replace_env(source, **values):
    env = dict(item.split("=", 1) for item in source["Config"]["Env"])
    env.update(values)
    source["Config"]["Env"] = [key + "=" + value for key, value in env.items()]


def snapshot(source=None):
    return release.snapshot(
        source if source is not None else source_inspect(),
        PROJECT, "127.0.0.1", 18910, PROXY_IMAGE,
    )


def counters(status, sessions=0, queued=0):
    return {"status": status, "sessions": sessions, "queued": queued}


def stats_csv(rows):
    lines = ["# pxname,svname,scur,qcur,status"]
    for slot, row in rows.items():
        lines.append(f"app,{slot},{row['sessions']},{row['queued']},{row['status']}")
    return "\n".join(lines) + "\n"


class FakeClock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        if seconds < 0 or len(self.sleeps) >= 30:
            raise AssertionError("Invalid sleep or unbounded polling in isolated test")
        self.sleeps.append(seconds)
        self.now += seconds


class IsolatedReleaseTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="rolling-release-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.events = []
        self.clock = FakeClock()
        self.patch(release, "time", SimpleNamespace(
            monotonic=self.clock.monotonic, sleep=self.clock.sleep,
        ))
        # A missed mock must fail locally, never launch Docker or open a socket.
        self.process = self.patch(release.subprocess, "run", side_effect=AssertionError(
            "Real subprocess execution is forbidden in these tests"
        ))
        # The production CLI requires Linux; its mocked socket path must also
        # be testable on Windows builds that do not expose AF_UNIX.
        self.patch(release.socket, "AF_UNIX", mock.sentinel.AF_UNIX, create=True)
        self.socket_factory = self.patch(release.socket, "socket", side_effect=AssertionError(
            "Real socket creation is forbidden in these tests"
        ))
        self.patch(release.socket, "create_connection", side_effect=AssertionError(
            "Real networking is forbidden in these tests"
        ))
        self.output = self.patch(release, "print", create=True)
        self.docker = self.patch(release, "run", side_effect=self.fake_docker)
        self.containers = {
            slot: {"Running": True, "Health": {"Status": "healthy"}}
            for slot in release.SLOTS
        }
        self.running_images = {slot: SOURCE_IMAGE for slot in release.SLOTS}
        self.edge = {"Status": "running", "Running": True}
        # Default: an older release whose /health lacks drain_mode_supported,
        # so no drain signal may ever be sent to it.
        self.health = {slot: {"status": "ok"} for slot in release.SLOTS}
        self.signals = []
        self.kill_fails = False
        self.rows ={"blue": counters("UP"), "green": counters("MAINT")}
        self.frames = []
        self.app = release.Release(self.root)
        self.app.state = snapshot()
        self.app.state["bootstrapped"] = True
        self.app.persist()
        self.app.render()
        self.control = self.patch(self.app, "socket_command", side_effect=self.fake_control)

    def patch(self, target, name, *args, **kwargs):
        patcher = mock.patch.object(target, name, *args, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def fake_docker(self, arguments, timeout=60):
        if arguments[:4] == ["docker", "inspect", "--type", "container"]:
            name = arguments[4]
            self.events.append(("inspect", name))
            if name == "synthetic-source":
                return json.dumps([source_inspect()])
            for slot in release.SLOTS:
                if name == PROJECT + "-" + slot:
                    return json.dumps([{"State": self.containers[slot], "Image": self.running_images[slot]}])
        elif arguments == ["docker", "container", "ls", "--all", "--quiet", "--filter",
                           f"name=^/{PROJECT}-edge$"]:
            self.events.append(("edge_lookup", PROJECT + "-edge"))
            return "synthetic-edge-id\n" if self.edge is not None else ""
        elif arguments == ["docker", "image", "inspect", "synthetic.invalid/app:initial"]:
            self.events.append(("image", "synthetic.invalid/app:initial"))
            return json.dumps([{"Id": SOURCE_IMAGE}])
        elif arguments == ["docker", "image", "inspect", CANDIDATE_REFERENCE]:
            self.events.append(("image", CANDIDATE_REFERENCE))
            return json.dumps([{"Id": CANDIDATE_IMAGE}])
        elif arguments == ["docker", "pull", CANDIDATE_REFERENCE]:
            self.events.append(("pull", CANDIDATE_REFERENCE))
            return ""
        elif arguments[:5] == ["docker", "compose", "--project-name", PROJECT, "-f"]:
            # Compose may only name a manifest inside this test's temporary root.
            Path(arguments[5]).relative_to(self.root)
            action = tuple(arguments[6:])
            self.events.append(("compose", action))
            if action == ("config", "--quiet"):
                return ""
            if action == ("up", "-d", "--no-deps", "--no-recreate", "edge"):
                self.edge = {"Status": "running", "Running": True}
                return ""
            for slot in release.SLOTS:
                if action == ("up", "-d", "--no-deps", slot):
                    self.containers[slot]["Running"] = True
                    self.running_images[slot] = self.app.state["slots"][slot]["image"]
                    return ""
                if action == ("up", "-d", "--no-deps", "--no-recreate", slot):
                    self.containers[slot]["Running"] = True
                    return ""
                if action == ("stop", "--timeout", "120", slot):
                    self.containers[slot]["Running"] = False
                    return ""
                if action == ("exec", "-T", slot, "wget", "-q", "-O", "-", "http://127.0.0.1:8187/health"):
                    reply = self.health[slot]
                    if reply is None or not self.containers[slot]["Running"]:
                        raise release.ReleaseError("Docker command failed")
                    return reply if isinstance(reply, str) else json.dumps(reply)
                for name in ("SIGUSR1", "SIGUSR2"):
                    if action == ("kill", "-s", name, slot):
                        if self.kill_fails:
                            raise release.ReleaseError("Docker command failed")
                        self.signals.append((slot, name))
                        return ""
        raise AssertionError("Unexpected Docker operation in isolated fixture")

    def fake_control(self, command):
        self.events.append(("proxy", command))
        if command == "show stat":
            return self.frames.pop(0) if self.frames else stats_csv(self.rows)
        for slot in release.SLOTS:
            for mode, status in (("ready", "UP"), ("drain", "DRAIN")):
                if command == f"set server app/{slot} state {mode}":
                    self.rows[slot]["status"] = status
                    return ""
        raise AssertionError("Unexpected proxy operation in isolated fixture")

    def disk_state(self):
        return json.loads(self.app.path.read_text(encoding="utf-8"))

    def manifest(self):
        return json.loads((self.root / "compose.json").read_text(encoding="utf-8"))

    def compose_actions(self):
        # Read-only /health probes are asserted separately (health_probes()).
        return [value for kind, value in self.events if kind == "compose" and value[:1] != ("exec",)]

    def health_probes(self):
        return [value[2] for kind, value in self.events if kind == "compose" and value[:1] == ("exec",)]

    def stops(self):
        return [action for action in self.compose_actions() if action[0] == "stop"]

    def cli(self, *arguments):
        # The real Linux lock and default state path are outside this unit test.
        with mock.patch.object(release, "locked", return_value=nullcontext()) as lock:
            with mock.patch.object(release, "Release", return_value=self.app) as constructor:
                result = release.main(["--state-dir", str(self.root), *arguments])
        lock.assert_called_once_with(str(self.root))
        constructor.assert_called_once_with(str(self.root))
        return result

    def use_socket_reply(self, *chunks):
        client = mock.MagicMock()
        client.__enter__.return_value = client
        client.recv.side_effect = [*chunks, b""]
        self.socket_factory.side_effect = None
        self.socket_factory.return_value = client
        self.control.side_effect = lambda command: release.Release.socket_command(self.app, command)
        return client


class SnapshotTests(IsolatedReleaseTest):
    def test_prepare_reads_synthetic_inspect_and_only_validates_compose(self):
        prepared = release.Release(self.root / "prepared")
        prepared.prepare("synthetic-source", PROJECT, "127.0.0.1", 18910, PROXY_IMAGE)
        self.assertEqual(self.events, [
            ("inspect", "synthetic-source"), ("compose", ("config", "--quiet")),
        ])
        saved = json.loads(prepared.path.read_text(encoding="utf-8"))
        self.assertEqual(saved["source_container"], "synthetic-source")
        self.assertFalse(saved["bootstrapped"])
        self.assertEqual(saved["slots"]["blue"]["image"], SOURCE_IMAGE)
        self.assertEqual(saved["slots"]["green"]["image"], SOURCE_IMAGE)
        self.process.assert_not_called()

    def test_shared_external_networks_and_volumes_and_bind_options_are_preserved(self):
        config = self.manifest()
        self.assertEqual(config["networks"], {
            "ingress": {"name": PROJECT + "-ingress"},
            "data0": {"external": True, "name": "synthetic-data"},
            "data1": {"external": True, "name": "synthetic-extra"},
        })
        self.assertEqual(config["volumes"], {
            "shared0": {"external": True, "name": "synthetic-assets"},
        })
        for slot in release.SLOTS:
            with self.subTest(slot=slot):
                service = config["services"][slot]
                self.assertEqual(service["networks"], ["data0", "data1", "ingress"])
                self.assertEqual(service["volumes"], [
                    {"type": "volume", "source": "shared0", "target": "/data", "read_only": False},
                    {"type": "bind", "source": "/synthetic/logs$$archive", "target": "/app/logs",
                     "read_only": True, "bind": {"create_host_path": False, "propagation": "rslave"}},
                ])

    def test_compose_escapes_literal_dollars_without_mutating_snapshot(self):
        original = copy.deepcopy(self.app.state)
        first = release.compose_config(self.app.state, self.root)
        second = release.compose_config(self.app.state, self.root)
        self.assertEqual(self.app.state, original)
        self.assertEqual(first, second)
        for slot in release.SLOTS:
            service = first["services"][slot]
            self.assertEqual(service["environment"]["SYNTHETIC_LITERAL"],
                             "one$$two$${THREE}$$$$four=value")
            self.assertEqual(service["environment"]["EMPTY"], "")
            self.assertEqual(service["command"], ["--label=synthetic$$label"])
        first["services"]["blue"]["environment"]["EMPTY"] = "changed"
        self.assertEqual(first["services"]["green"]["environment"]["EMPTY"], "")
        self.assertEqual(self.app.state, original)

    def test_manifest_has_only_app_slots_and_edge_without_application_ports(self):
        services = self.manifest()["services"]
        self.assertEqual(set(services), {"blue", "green", "edge"})
        for slot in release.SLOTS:
            service = services[slot]
            for omitted in ("ports", "network_mode", "depends_on", "env_file", "build"):
                self.assertNotIn(omitted, service)
            self.assertEqual(service["image"], SOURCE_IMAGE)
        self.assertEqual(services["edge"]["ports"], [
            {"target": 8080, "published": "18910", "host_ip": "127.0.0.1", "protocol": "tcp"},
        ])
        self.assertEqual(services["edge"]["networks"], ["ingress"])

    def test_both_slots_health_checks_and_proxy_keep_source_internal_port(self):
        config = self.manifest()
        proxy = release.haproxy_config(self.app.state)
        self.assertEqual(self.app.state["app_port"], 8187)
        for slot in release.SLOTS:
            service = config["services"][slot]
            self.assertEqual(service["environment"]["CODEX_PORT"], "8187")
            self.assertEqual(service["environment"]["CODEX_BIND"], "0.0.0.0")
            self.assertEqual(service["healthcheck"]["test"][-1], "http://127.0.0.1:8187/health")
            self.assertIn(f"server {slot} {PROJECT}-{slot}:8187 check", proxy)
        server_lines = [line for line in proxy.splitlines() if line.strip().startswith("server ")]
        self.assertFalse(server_lines[0].endswith(" disabled"))
        self.assertTrue(server_lines[1].endswith(" disabled"))

    def test_proxy_connection_limits_are_explicit_on_frontend_and_backend(self):
        lines = [line.strip() for line in release.haproxy_config(self.app.state).splitlines()]
        limit = f"maxconn {release.PROXY_MAXCONN}"
        frontend, backend = lines.index("frontend public"), lines.index("backend app")
        self.assertIn(limit, lines[:lines.index("defaults")])
        self.assertIn(limit, lines[frontend:backend])
        self.assertIn(f"fullconn {release.PROXY_MAXCONN}", lines[backend:])

    def test_internal_port_uses_documented_port_fallback_when_codex_port_is_absent(self):
        source = source_inspect()
        source["Config"]["Env"] = [
            item for item in source["Config"]["Env"] if not item.startswith("CODEX_PORT=")
        ]
        replace_env(source, PORT="10000")
        state = snapshot(source)
        self.assertEqual(state["app_port"], 10000)
        config = release.compose_config(state, self.root)
        for slot in release.SLOTS:
            self.assertEqual(config["services"][slot]["healthcheck"]["test"][-1],
                             "http://127.0.0.1:10000/health")
            self.assertIn(f"server {slot} {PROJECT}-{slot}:10000 check",
                          release.haproxy_config(state))

    def test_snapshot_does_not_change_inspect_input(self):
        source = source_inspect()
        before = copy.deepcopy(source)
        snapshot(source)
        self.assertEqual(source, before)

    def test_sqlite_and_memory_cache_are_rejected(self):
        for env in ({"DATABASE_DRIVER": "sqlite"}, {"DATABASE_DRIVER": "SQLITE"},
                    {"CACHE_DRIVER": "memory"}, {"CACHE_DRIVER": "MEMORY"}):
            with self.subTest(env=env):
                source = source_inspect()
                replace_env(source, **env)
                with self.assertRaises(release.ReleaseError):
                    snapshot(source)
        self.docker.assert_not_called()

    def test_shared_database_and_cache_addresses_are_required(self):
        for name in ("DATABASE_HOST", "REDIS_ADDR"):
            with self.subTest(name=name):
                source = source_inspect()
                replace_env(source, **{name: ""})
                with self.assertRaises(release.ReleaseError):
                    snapshot(source)

    def test_explicit_background_image_workers_are_rejected(self):
        for workers in ("1", " 2 ", "-1", "invalid"):
            with self.subTest(workers=workers):
                source = source_inspect()
                replace_env(source, IMAGE_JOB_WORKERS=workers)
                with self.assertRaisesRegex(release.ReleaseError, "background image workers"):
                    snapshot(source)

    def test_missing_declared_mount_tmpfs_and_missing_shared_network_are_rejected(self):
        sources = []
        missing_mount = source_inspect()
        missing_mount["Mounts"] = []
        sources.append(missing_mount)
        tmpfs = source_inspect()
        tmpfs["HostConfig"]["Tmpfs"] = {"/tmp": "rw"}
        sources.append(tmpfs)
        no_network = source_inspect()
        no_network["NetworkSettings"]["Networks"] = {}
        sources.append(no_network)
        for index, source in enumerate(sources):
            with self.subTest(case=index), self.assertRaises(release.ReleaseError):
                snapshot(source)

    def test_image_resolution_pins_immutable_id_with_optional_mocked_pull(self):
        for pull in (False, True):
            with self.subTest(pull=pull):
                self.events.clear()
                resolved = self.app.resolve_image(CANDIDATE_REFERENCE, pull)
                self.assertEqual(resolved, {"reference": CANDIDATE_REFERENCE, "image": CANDIDATE_IMAGE})
                expected = [("image", CANDIDATE_REFERENCE)]
                if pull:
                    expected.insert(0, ("pull", CANDIDATE_REFERENCE))
                self.assertEqual(self.events, expected)
        self.process.assert_not_called()


class DrainTests(IsolatedReleaseTest):
    def test_replace_gate_requires_disabled_or_draining_and_both_counters_zero(self):
        for status, sessions, queued, allowed in (
            ("DRAIN", 0, 0, True), ("MAINT", 0, 0, True),
            ("UP", 0, 0, False), ("DOWN", 0, 0, False), ("NOLB", 0, 0, False),
            ("DRAIN", 1, 0, False), ("MAINT", 0, 1, False), ("DRAIN", 1, 1, False),
        ):
            with self.subTest(status=status, sessions=sessions, queued=queued):
                self.rows["green"] = counters(status, sessions, queued)
                if allowed:
                    self.app.safe_to_replace("green")
                else:
                    with self.assertRaises(release.ReleaseError):
                        self.app.safe_to_replace("green")
        self.docker.assert_not_called()

    def test_stop_requires_disabled_or_draining_and_zero_sessions_and_queue(self):
        for status in ("DRAIN", "MAINT", "MAINT (resolution)"):
            with self.subTest(status=status):
                self.events.clear()
                self.containers["green"]["Running"] = True
                self.rows["green"] = counters(status)
                self.assertIs(self.app.drain(0), True)
                self.assertEqual(self.events, [
                    ("proxy", "show stat"), ("compose", ("stop", "--timeout", "120", "green")),
                ])
                self.assertTrue(self.containers["blue"]["Running"])
                self.assertFalse(self.containers["green"]["Running"])

    def test_one_sse_or_websocket_connection_times_out_false_and_keeps_old_running(self):
        # Both protocols are represented by one HAProxy session; no protocol I/O.
        for protocol in ("SSE", "WebSocket"):
            with self.subTest(protocol=protocol):
                self.rows["green"] = counters("DRAIN", sessions=1)
                started = self.clock.now
                self.assertIs(self.app.drain(3, force_stop=False), False)
                self.assertEqual(self.clock.now - started, 3)
                self.assertTrue(self.containers["green"]["Running"])
                self.assertTrue(self.containers["blue"]["Running"])
        self.assertEqual(self.compose_actions(), [])
        self.assertEqual(self.signals, [])

    def test_one_queued_request_with_zero_sessions_also_prevents_stop(self):
        self.rows["green"] = counters("MAINT", queued=1)
        self.assertIs(self.app.drain(0, force_stop=False), False)
        self.assertTrue(self.containers["green"]["Running"])
        self.assertEqual(self.compose_actions(), [])

    def test_polling_waits_for_sessions_and_queue_before_stop(self):
        self.frames = [stats_csv({"blue": counters("UP"), "green": row}) for row in (
            counters("DRAIN", sessions=1), counters("DRAIN", queued=1), counters("DRAIN"),
        )]
        self.assertIs(self.app.drain(10), True)
        probe = ("exec", "-T", "green", "wget", "-q", "-O", "-", "http://127.0.0.1:8187/health")
        self.assertEqual(self.events, [
            ("proxy", "show stat"), ("compose", probe), ("proxy", "show stat"), ("proxy", "show stat"),
            ("compose", ("stop", "--timeout", "120", "green")),
        ])
        self.assertEqual(self.clock.sleeps, [2, 2])

    def test_default_drain_timeout_is_three_minutes(self):
        self.assertEqual(release.DRAIN_TIMEOUT, 180)
        for arguments in (["deploy", CANDIDATE_REFERENCE], ["rollback"], ["drain"]):
            with self.subTest(action=arguments[0]):
                parsed = release.parser().parse_args(arguments)
                self.assertEqual(parsed.drain_timeout, 180)
                self.assertIs(parsed.force_stop, True)
                parsed = release.parser().parse_args([*arguments, "--no-force-stop"])
                self.assertIs(parsed.force_stop, False)

    def test_timeout_force_stops_only_the_inactive_draining_slot(self):
        for row in (counters("DRAIN", sessions=1), counters("MAINT", queued=2), counters("DRAIN", 3, 4)):
            with self.subTest(row=row):
                self.events.clear()
                self.output.reset_mock()
                self.containers["green"]["Running"] = True
                self.rows["green"] = dict(row)
                started = self.clock.now
                self.assertIs(self.app.drain(5), True)
                self.assertEqual(self.clock.now - started, 5)
                self.assertEqual(self.stops(), [("stop", "--timeout", "120", "green")])
                self.assertFalse(self.containers["green"]["Running"])
                self.assertTrue(self.containers["blue"]["Running"])
                printed = " ".join(str(call.args[0]) for call in self.output.call_args_list)
                self.assertIn("force stopping green", printed)
                self.assertIn(f"{row['sessions']} connections and {row['queued']} queued", printed)

    def test_zero_timeout_with_connections_force_stops_by_default(self):
        self.rows["green"] = counters("DRAIN", sessions=1)
        self.assertIs(self.app.drain(0), True)
        self.assertEqual(self.stops(), [("stop", "--timeout", "120", "green")])
        self.assertEqual(self.clock.sleeps, [])

    def test_force_stop_keeps_safety_gates(self):
        # Timed out with connections, but the active slot is not UP or the old
        # slot still accepts traffic: never force stop anything.
        for active, old in (("DOWN", "DRAIN"), ("MAINT", "DRAIN"), ("UP", "UP"), ("UP", "NOLB")):
            with self.subTest(active=active, old=old):
                self.rows = {"blue": counters(active, sessions=1), "green": counters(old, sessions=1)}
                with self.assertRaises(release.ReleaseError):
                    self.app.drain(0)
                self.assertEqual(self.stops(), [])
                self.assertTrue(all(item["Running"] for item in self.containers.values()))

    def test_force_stop_gate_rechecked_on_the_final_poll(self):
        self.frames = [
            stats_csv({"blue": counters("UP"), "green": counters("DRAIN", sessions=1)}),
            stats_csv({"blue": counters("DOWN"), "green": counters("DRAIN", sessions=1)}),
        ]
        with self.assertRaises(release.ReleaseError):
            self.app.drain(2)
        self.assertEqual(self.stops(), [])

    def test_active_not_exactly_up_prevents_stopping_idle_old_slot(self):
        for status in ("DOWN", "DRAIN", "MAINT", "UP 1/2", ""):
            with self.subTest(status=status):
                self.rows["blue"]["status"] = status
                with self.assertRaises(release.ReleaseError):
                    self.app.drain(0)
                self.assertTrue(self.containers["green"]["Running"])
        self.docker.assert_not_called()

    def test_old_slot_still_accepting_traffic_cannot_be_stopped(self):
        for status in ("UP", "DOWN", "NOLB"):
            with self.subTest(status=status):
                self.rows["green"] = counters(status)
                with self.assertRaises(release.ReleaseError):
                    self.app.drain(0)
        self.docker.assert_not_called()

    def test_pending_journal_blocks_deployment_and_other_mutating_commands(self):
        self.app.state["pending"] = {"previous": "blue", "target": "green"}
        self.app.persist()
        before = self.disk_state()
        restarted = release.Release(self.root)
        self.assertEqual(restarted.state, before)
        for operation in (
            lambda: restarted.deploy(CANDIDATE_REFERENCE, True, 0, 0),
            lambda: self.app.rollback(0, 0), lambda: self.app.drain(0),
            lambda: self.app.bootstrap(CANDIDATE_REFERENCE, True, 0),
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(release.ReleaseError, "recover"):
                operation()
        self.assertEqual(self.app.state, before)
        self.assertEqual(self.disk_state(), before)
        self.control.assert_not_called()
        self.docker.assert_not_called()


class BootstrapRecoveryTests(IsolatedReleaseTest):
    def test_unhealthy_first_image_can_be_replaced_before_edge_exists(self):
        self.app.state["bootstrapped"] = False
        self.edge = None
        self.containers["blue"]["Health"]["Status"] = "unhealthy"
        with self.assertRaisesRegex(release.ReleaseError, "did not become healthy"):
            self.app.bootstrap("synthetic.invalid/app:initial", False, 0)
        self.assertFalse(self.disk_state()["bootstrapped"])
        self.assertFalse(self.disk_state().get("bootstrap_edge_started"))
        self.assertEqual(self.disk_state()["bootstrap_image"]["image"], SOURCE_IMAGE)
        self.assertEqual(self.compose_actions(), [("up", "-d", "--no-deps", "blue")])
        self.assertIsNone(self.edge)
        self.control.assert_not_called()

        # A fresh process must be able to replace the failed candidate container.
        self.app = release.Release(self.root)
        self.control = self.patch(self.app, "socket_command", side_effect=self.fake_control)
        self.events.clear()
        self.containers["blue"]["Health"]["Status"] = "healthy"
        self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
        pinned = {"image": CANDIDATE_IMAGE, "reference": CANDIDATE_REFERENCE}
        self.assertTrue(self.disk_state()["bootstrapped"])
        self.assertTrue(self.disk_state()["bootstrap_edge_started"])
        self.assertEqual(self.disk_state()["bootstrap_image"], pinned)
        self.assertEqual(self.disk_state()["slots"]["blue"], pinned)
        self.assertEqual(self.running_images["blue"], CANDIDATE_IMAGE)
        self.assertEqual(self.running_images["green"], SOURCE_IMAGE)
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "blue"),
            ("up", "-d", "--no-deps", "--no-recreate", "edge"),
        ])
        self.assertEqual(self.events[0], ("edge_lookup", PROJECT + "-edge"))

    def test_interrupted_pre_edge_replacement_reuses_saved_id_and_recreates_candidate(self):
        # Crash after saving the replacement, before Compose updates old blue.
        self.app.state["bootstrapped"] = False
        pinned = {"image": CANDIDATE_IMAGE, "reference": CANDIDATE_REFERENCE}
        self.app.state["bootstrap_image"] = copy.deepcopy(pinned)
        self.app.state["slots"]["blue"] = copy.deepcopy(pinned)
        self.app.persist()
        self.edge = None
        self.app.bootstrap(CANDIDATE_REFERENCE, True, 0)
        self.assertTrue(self.disk_state()["bootstrapped"])
        self.assertEqual(self.running_images["blue"], CANDIDATE_IMAGE)
        self.assertEqual(self.compose_actions()[0], ("up", "-d", "--no-deps", "blue"))
        self.assertFalse(any(kind in ("image", "pull") for kind, _ in self.events))

    def test_interrupted_bootstrap_resumes_pinned_image_without_recreating(self):
        for status in ("created", "running", "exited"):
            with self.subTest(edge_status=status):
                self.events.clear()
                self.edge = {"Status": status, "Running": status == "running"}
                self.app.state["bootstrapped"] = False
                self.app.state.pop("bootstrap_edge_started", None)
                pinned = {"image": SOURCE_IMAGE, "reference": CANDIDATE_REFERENCE}
                self.app.state["bootstrap_image"] = pinned
                self.rows["blue"]["sessions"] = 1
                self.app.persist()
                self.app.bootstrap(CANDIDATE_REFERENCE, True, 0)
                self.assertTrue(self.app.state["bootstrapped"])
                self.assertEqual(self.running_images["blue"], SOURCE_IMAGE)
                self.assertEqual(self.app.state["slots"]["blue"], pinned)
                self.assertEqual(self.compose_actions(), [
                    ("up", "-d", "--no-deps", "--no-recreate", "blue"),
                    ("up", "-d", "--no-deps", "--no-recreate", "edge"),
                ])
                self.assertFalse(any(kind in ("image", "pull") for kind, _ in self.events))
                self.assertEqual(self.rows["blue"]["sessions"], 1)

    def test_interrupted_bootstrap_rejects_different_image(self):
        for status in ("created", "running", "exited"):
            with self.subTest(edge_status=status):
                self.events.clear()
                self.edge = {"Status": status, "Running": status == "running"}
                self.app.state["bootstrapped"] = False
                self.app.state["bootstrap_image"] = {
                    "image": SOURCE_IMAGE, "reference": "synthetic.invalid/app:initial",
                }
                self.app.persist()
                before = self.disk_state()
                with self.assertRaisesRegex(release.ReleaseError, "Bootstrap already started"):
                    self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
                self.assertEqual(self.events, [("edge_lookup", PROJECT + "-edge")])
                self.assertEqual(self.disk_state(), before)
                self.assertEqual(self.app.state, before)
                self.control.assert_not_called()

    def test_edge_lookup_failure_is_not_container_absence(self):
        self.app.state["bootstrapped"] = False
        self.app.state["bootstrap_image"] = {
            "image": SOURCE_IMAGE, "reference": "synthetic.invalid/app:initial",
        }
        self.app.persist()
        before = self.disk_state()
        failure = release.ReleaseError("synthetic Docker query failure")
        self.docker.side_effect = failure
        with self.assertRaises(release.ReleaseError) as raised:
            self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
        self.assertIs(raised.exception, failure)
        self.docker.assert_called_once_with([
            "docker", "container", "ls", "--all", "--quiet", "--filter",
            f"name=^/{PROJECT}-edge$",
        ])
        self.assertEqual(self.disk_state(), before)
        self.assertEqual(self.app.state, before)
        self.control.assert_not_called()

    def test_health_inspect_failure_does_not_unpin_existing_edge(self):
        self.app.state["bootstrapped"] = False
        pinned = {"image": SOURCE_IMAGE, "reference": CANDIDATE_REFERENCE}
        self.app.state["bootstrap_image"] = pinned
        self.app.persist()
        self.patch(release, "inspect_container", side_effect=release.ReleaseError("synthetic inspect failure"))
        with self.assertRaisesRegex(release.ReleaseError, "did not become healthy"):
            self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
        self.assertEqual(self.disk_state()["bootstrap_image"], pinned)
        self.assertTrue(self.disk_state()["bootstrap_edge_started"])
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "--no-recreate", "blue"),
        ])
        self.control.assert_not_called()

    def test_edge_start_failure_keeps_pin_before_container_creation(self):
        self.app.state["bootstrapped"] = False
        self.edge = None
        compose = self.app.compose

        def fail_edge(*args, **kwargs):
            if args[-1] == "edge":
                self.assertTrue(self.disk_state()["bootstrap_edge_started"])
                raise release.ReleaseError("synthetic edge start failure")
            return compose(*args, **kwargs)

        with mock.patch.object(self.app, "compose", side_effect=fail_edge):
            with self.assertRaisesRegex(release.ReleaseError, "synthetic edge start failure"):
                self.app.bootstrap("synthetic.invalid/app:initial", False, 0)
        self.assertIsNone(self.edge)
        self.app = release.Release(self.root)
        self.control = self.patch(self.app, "socket_command", side_effect=self.fake_control)
        self.events.clear()
        with self.assertRaisesRegex(release.ReleaseError, "Bootstrap already started"):
            self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
        self.assertEqual(self.events, [])
        self.app.bootstrap("synthetic.invalid/app:initial", True, 0)
        self.assertEqual(self.running_images["blue"], SOURCE_IMAGE)
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "--no-recreate", "blue"),
            ("up", "-d", "--no-deps", "--no-recreate", "edge"),
        ])

    def test_proxy_readiness_failure_remembers_started_edge_after_removal(self):
        self.app.state["bootstrapped"] = False
        self.edge = None
        self.rows["blue"] = counters("DOWN", sessions=1)
        with self.assertRaisesRegex(release.ReleaseError, "Proxy readiness timed out"):
            self.app.bootstrap("synthetic.invalid/app:initial", False, 0)
        self.assertTrue(self.edge["Running"])
        self.assertTrue(self.disk_state()["bootstrap_edge_started"])
        self.assertFalse(self.disk_state()["bootstrapped"])
        self.app = release.Release(self.root)
        self.control = self.patch(self.app, "socket_command", side_effect=self.fake_control)
        self.edge = None
        self.events.clear()
        with self.assertRaisesRegex(release.ReleaseError, "Bootstrap already started"):
            self.app.bootstrap(CANDIDATE_REFERENCE, False, 0)
        self.assertEqual(self.events, [])
        self.rows["blue"]["status"] = "UP"
        self.app.bootstrap("synthetic.invalid/app:initial", False, 0)
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "--no-recreate", "blue"),
            ("up", "-d", "--no-deps", "--no-recreate", "edge"),
        ])
        self.assertEqual(self.running_images["blue"], SOURCE_IMAGE)


class DeploymentTests(IsolatedReleaseTest):
    def test_successful_deploy_pins_image_confirms_routing_then_stops_only_old(self):
        self.assertIs(self.app.deploy(CANDIDATE_REFERENCE, False, 5, 5), True)
        self.assertEqual(self.app.state["active"], "green")
        self.assertIsNone(self.app.state["pending"])
        self.assertEqual(self.disk_state(), self.app.state)
        self.assertEqual(self.manifest()["services"]["green"]["image"], CANDIDATE_IMAGE)
        self.assertEqual(self.manifest()["services"]["blue"]["image"], SOURCE_IMAGE)
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "green"), ("stop", "--timeout", "120", "blue"),
        ])
        ready = self.events.index(("proxy", "set server app/green state ready"))
        draining = self.events.index(("proxy", "set server app/blue state drain"))
        self.assertLess(self.events.index(("inspect", PROJECT + "-green")), ready)
        self.assertLess(ready, draining)
        self.assertIn(("proxy", "show stat"), self.events[ready + 1:draining])
        self.assertIn(("proxy", "show stat"), self.events[draining + 1:-1])
        self.assertFalse(self.containers["blue"]["Running"])
        self.assertTrue(self.containers["green"]["Running"])

    def test_failed_new_health_never_switches_current_or_stops_old(self):
        for health in ("starting", "unhealthy"):
            with self.subTest(health=health):
                self.events.clear()
                self.containers["green"]["Health"]["Status"] = health
                before_proxy = (self.root / "proxy" / "haproxy.cfg").read_text(encoding="utf-8")
                with self.assertRaisesRegex(release.ReleaseError, "did not become healthy"):
                    self.app.deploy(CANDIDATE_REFERENCE, False, 4, 0)
                self.assertEqual(self.app.state["active"], "blue")
                self.assertEqual(self.disk_state()["active"], "blue")
                self.assertIsNone(self.disk_state()["pending"])
                self.assertEqual(self.rows["blue"]["status"], "UP")
                self.assertEqual(self.rows["green"]["status"], "MAINT")
                self.assertEqual(self.compose_actions(), [("up", "-d", "--no-deps", "green")])
                self.assertFalse(any(kind == "proxy" and value.startswith("set server")
                                     for kind, value in self.events))
                self.assertEqual((self.root / "proxy" / "haproxy.cfg").read_text(encoding="utf-8"),
                                 before_proxy)
                self.assertTrue(self.containers["blue"]["Running"])

    def test_deploy_rejects_live_candidate_before_image_resolution_or_recreation(self):
        self.rows["green"] = counters("DRAIN", sessions=1)
        before = copy.deepcopy(self.app.state)
        with self.assertRaises(release.ReleaseError):
            self.app.deploy(CANDIDATE_REFERENCE, True, 0, 0)
        self.docker.assert_not_called()
        self.assertEqual(self.app.state, before)
        self.assertEqual(self.disk_state(), before)

    def test_unconfirmed_switch_keeps_journal_and_both_containers(self):
        # Readiness succeeds, but the subsequent observation says the old slot
        # still accepts traffic despite the command's empty acknowledgement.
        both_up = stats_csv({"blue": counters("UP"), "green": counters("UP")})
        self.frames = [both_up, both_up]
        with self.assertRaisesRegex(release.ReleaseError, "Switch not confirmed"):
            self.app.switch("green", 0)
        self.assertEqual(self.app.state["active"], "blue")
        self.assertEqual(self.disk_state()["pending"], {"target": "green", "previous": "blue"})
        self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.docker.assert_not_called()

    def test_proxy_rejection_keeps_pending_journal_without_stopping_containers(self):
        self.control.side_effect = None
        self.control.return_value = "synthetic proxy rejection\n"
        with self.assertRaisesRegex(release.ReleaseError, "rejected"):
            self.app.switch("green", 0)
        self.assertEqual(self.disk_state()["active"], "blue")
        self.assertEqual(self.disk_state()["pending"], {"target": "green", "previous": "blue"})
        self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.docker.assert_not_called()

    def cli_with_old_connection(self, action, *extra):
        self.rows["blue"]["sessions"] = 1
        self.rows["green"]["sessions"] = 1 if action == "drain" else 0
        arguments = [action]
        if action == "deploy":
            arguments.append(CANDIDATE_REFERENCE)
        arguments.extend(["--drain-timeout", "0", *extra])
        result = self.cli(*arguments)
        self.assertEqual(self.app.state["active"], "blue" if action == "drain" else "green")
        return result

    def assert_cli_pending(self, action):
        self.assertEqual(self.cli_with_old_connection(action, "--no-force-stop"), 2)
        self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.assertEqual(self.stops(), [])

    def assert_cli_force_stopped(self, action):
        self.assertEqual(self.cli_with_old_connection(action), 0)
        old = "green" if action == "drain" else "blue"
        self.assertEqual(self.stops(), [("stop", "--timeout", "120", old)])
        self.assertFalse(self.containers[old]["Running"])
        self.assertTrue(self.containers["green" if old == "blue" else "blue"]["Running"])

    def test_deploy_cli_returns_two_when_old_connection_remains(self):
        self.assert_cli_pending("deploy")

    def test_drain_cli_returns_two_when_old_connection_remains(self):
        self.assert_cli_pending("drain")

    def test_rollback_cli_returns_two_when_old_connection_remains(self):
        self.assert_cli_pending("rollback")

    def test_deploy_cli_force_stops_after_timeout_by_default(self):
        self.assert_cli_force_stopped("deploy")

    def test_drain_cli_force_stops_after_timeout_by_default(self):
        self.assert_cli_force_stopped("drain")

    def test_rollback_cli_force_stops_after_timeout_by_default(self):
        self.assert_cli_force_stopped("rollback")


class DrainSignalTests(IsolatedReleaseTest):
    SUPPORTED = {"status": "ok", "drain_mode_supported": True, "draining": False}

    def test_switch_skips_signals_for_release_without_drain_mode_field(self):
        self.app.switch("green", 0)
        self.assertEqual(self.signals, [])
        self.assertEqual(self.health_probes(), ["green", "blue"])
        printed = " ".join(str(call.args[0]) for call in self.output.call_args_list)
        self.assertIn("SIGUSR1 to blue skipped", printed)
        self.assertIn("SIGUSR2 to green skipped", printed)

    def test_unreadable_or_invalid_health_never_signals(self):
        for reply in (None, "not json", "[1]", {"status": "ok", "drain_mode_supported": "true"},
                      {"status": "ok", "drain_mode_supported": False}):
            with self.subTest(reply=reply):
                self.health = {slot: reply for slot in release.SLOTS}
                self.assertIs(self.app.signal_drain_mode("blue", enter=True), False)
                self.assertEqual(self.signals, [])

    def test_switch_signals_previous_to_enter_and_target_to_exit(self):
        self.health = {slot: dict(self.SUPPORTED) for slot in release.SLOTS}
        self.app.switch("green", 0)
        self.assertEqual(self.signals, [("green", "SIGUSR2"), ("blue", "SIGUSR1")])
        # Signals follow the confirmed and persisted switch.
        self.assertEqual(self.disk_state()["active"], "green")
        kills = [i for i, (kind, value) in enumerate(self.events) if kind == "compose" and value[0] == "kill"]
        drain = self.events.index(("proxy", "set server app/blue state drain"))
        self.assertTrue(all(index > drain for index in kills))

    def test_only_supported_slot_is_signalled(self):
        self.health = {"blue": dict(self.SUPPORTED), "green": {"status": "ok"}}
        self.app.switch("green", 0)
        self.assertEqual(self.signals, [("blue", "SIGUSR1")])

    def test_failed_signal_does_not_fail_deploy(self):
        self.health = {slot: dict(self.SUPPORTED) for slot in release.SLOTS}
        self.kill_fails = True
        self.assertIs(self.app.deploy(CANDIDATE_REFERENCE, False, 5, 5), True)
        self.assertEqual(self.app.state["active"], "green")
        self.assertEqual(self.stops(), [("stop", "--timeout", "120", "blue")])

    def test_drain_resignals_old_slot_only_while_it_has_connections(self):
        self.health = {slot: dict(self.SUPPORTED) for slot in release.SLOTS}
        self.rows["green"] = counters("DRAIN", sessions=1)
        self.assertIs(self.app.drain(4), True)
        self.assertEqual(self.signals, [("green", "SIGUSR1")])
        self.signals.clear()
        self.rows["green"] = counters("DRAIN")
        self.containers["green"]["Running"] = True
        self.assertIs(self.app.drain(4), True)
        self.assertEqual(self.signals, [])

    def test_unconfirmed_switch_sends_no_signal(self):
        self.health = {slot: dict(self.SUPPORTED) for slot in release.SLOTS}
        both_up = stats_csv({"blue": counters("UP"), "green": counters("UP")})
        self.frames = [both_up, both_up]
        with self.assertRaises(release.ReleaseError):
            self.app.switch("green", 0)
        self.assertEqual(self.signals, [])

    def test_recover_abort_signals_restored_active_to_exit_and_candidate_to_enter(self):
        self.health = {slot: dict(self.SUPPORTED) for slot in release.SLOTS}
        self.app.state["active"] = "green"
        self.app.state["pending"] = {"previous": "blue", "target": "green"}
        self.app.persist()
        self.rows = {"blue": counters("DRAIN", sessions=1), "green": counters("UP", sessions=1)}
        self.app.recover(0, abort=True)
        self.assertEqual(self.signals, [("blue", "SIGUSR2"), ("green", "SIGUSR1")])

    def test_status_reports_draining_when_available(self):
        self.health = {"blue": dict(self.SUPPORTED), "green": dict(self.SUPPORTED, draining=True)}
        self.containers["green"]["Running"] = True
        self.app.status()
        report = json.loads(self.output.call_args.args[0])
        self.assertEqual(report["draining"], {"blue": False, "green": True})
        self.health = {"blue": {"status": "ok"}, "green": None}
        self.app.status()
        report = json.loads(self.output.call_args.args[0])
        self.assertEqual(report["draining"], {"blue": None, "green": None})
        self.assertEqual(self.signals, [])


class RollbackRecoveryTests(IsolatedReleaseTest):
    def test_first_failed_deploy_keeps_original_source_as_rollback_target(self):
        self.app.state["slots"]["green"] = {"image": CANDIDATE_IMAGE, "reference": CANDIDATE_REFERENCE}
        self.running_images["green"] = CANDIDATE_IMAGE
        self.app.persist()
        self.app.render()
        self.assertIs(self.app.rollback(5, 0), True)
        self.assertEqual(self.app.state["active"], "green")
        self.assertEqual(self.running_images["green"], SOURCE_IMAGE)
        self.assertEqual(self.manifest()["services"]["green"]["image"], SOURCE_IMAGE)

    def test_interrupted_rollback_rebuilds_manifest_and_checks_actual_running_image(self):
        self.app.state["active"] = "green"
        self.rows = {"blue": counters("MAINT"), "green": counters("UP")}
        failed = {"image": "sha256:" + "4" * 64, "reference": "synthetic.invalid/app:failed"}
        good = copy.deepcopy(self.app.state["slots"]["blue"])
        self.app.state["slots"]["blue"] = failed
        self.app.render()
        self.running_images["blue"] = failed["image"]
        # Crash after saving the good image, but before rendering/recreating.
        self.app.state["slots"]["blue"] = good
        self.app.state["previous_release"] = {"slot": "blue", "release": good}
        self.app.persist()
        self.assertFalse(self.app.healthy("blue"))
        self.assertIs(self.app.rollback(5, 0), True)
        self.assertEqual(self.running_images["blue"], SOURCE_IMAGE)
        self.assertEqual(self.manifest()["services"]["blue"]["image"], SOURCE_IMAGE)

    def test_failed_candidate_does_not_replace_saved_successful_rollback_image(self):
        self.app.state["active"] = "green"
        self.app.state["slots"]["green"] = {"image": CANDIDATE_IMAGE, "reference": CANDIDATE_REFERENCE}
        self.app.state["previous_release"] = {"slot": "blue", "release": copy.deepcopy(self.app.state["slots"]["blue"])}
        self.app.state["slots"]["blue"] = {"image": "sha256:" + "4" * 64, "reference": "synthetic.invalid/app:failed"}
        self.app.persist()
        self.app.render()
        self.rows = {"blue": counters("MAINT"), "green": counters("UP")}
        # The failed candidate has since become healthy, but was never committed.
        self.assertIs(self.app.rollback(5, 0), True)
        self.assertEqual(self.app.state["active"], "blue")
        self.assertEqual(self.manifest()["services"]["blue"]["image"], SOURCE_IMAGE)
        self.assertIn(("up", "-d", "--no-deps", "blue"), self.compose_actions())
        self.assertEqual(self.app.state["previous_release"]["release"]["image"], CANDIDATE_IMAGE)

    def test_rollback_reuses_healthy_live_target_without_recreating_it(self):
        self.rows["green"] = counters("DRAIN", sessions=1)
        self.rows["blue"]["sessions"] = 1
        before_slots = copy.deepcopy(self.app.state["slots"])
        self.assertIs(self.app.rollback(0, 0, force_stop=False), False)
        self.assertEqual(self.app.state["active"], "green")
        self.assertEqual(self.app.state["slots"], before_slots)
        self.assertEqual(self.compose_actions(), [])
        self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.assertEqual(self.rows["green"]["sessions"], 1)

    def test_rollback_will_not_recreate_unhealthy_target_with_connections_or_queue(self):
        self.containers["green"]["Health"]["Status"] = "unhealthy"
        for row in (counters("DRAIN", sessions=1), counters("MAINT", queued=1)):
            with self.subTest(row=row):
                self.rows["green"] = row
                with self.assertRaises(release.ReleaseError):
                    self.app.rollback(0, 0)
                self.assertEqual(self.app.state["active"], "blue")
        self.assertEqual(self.compose_actions(), [])
        self.assertTrue(all(item["Running"] for item in self.containers.values()))

    def test_rollback_starts_stopped_safe_target_then_drains_previous(self):
        self.containers["green"]["Running"] = False
        self.assertIs(self.app.rollback(0, 0), True)
        self.assertEqual(self.app.state["active"], "green")
        self.assertEqual(self.compose_actions(), [
            ("up", "-d", "--no-deps", "green"), ("stop", "--timeout", "120", "blue"),
        ])
        self.assertTrue(self.containers["green"]["Running"])
        self.assertFalse(self.containers["blue"]["Running"])

    def test_recover_abort_cli_restores_recorded_original_and_retains_connections(self):
        self.app.state["active"] = "green"
        self.app.state["pending"] = {"previous": "blue", "target": "green"}
        self.app.persist()
        self.app.render()
        self.rows = {"blue": counters("DRAIN", sessions=1), "green": counters("UP", sessions=1)}
        self.assertEqual(self.cli("recover", "--abort", "--ready-timeout", "0"), 0)
        self.assertEqual(self.app.state["active"], "blue")
        self.assertIsNone(self.app.state["pending"])
        self.assertEqual(self.disk_state(), self.app.state)
        self.assertEqual(self.compose_actions(), [])
        self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.assertEqual(self.rows, {"blue": counters("UP", sessions=1),
                                     "green": counters("DRAIN", sessions=1)})
        self.assertEqual(self.control.call_args_list, [
            mock.call("set server app/blue state ready"), mock.call("show stat"),
            mock.call("set server app/green state drain"),
        ])
        proxy = (self.root / "proxy" / "haproxy.cfg").read_text(encoding="utf-8")
        self.assertEqual(proxy, release.haproxy_config(self.app.state, "blue"))

    def test_recover_abort_requires_original_health_before_routing_changes(self):
        self.app.state["pending"] = {"previous": "blue", "target": "green"}
        self.app.persist()
        self.containers["blue"]["Health"]["Status"] = "unhealthy"
        before = self.disk_state()
        with self.assertRaises(release.ReleaseError):
            self.app.recover(0, abort=True)
        self.assertEqual(self.disk_state(), before)
        self.control.assert_not_called()
        self.assertEqual(self.compose_actions(), [])


class SocketStatsTests(IsolatedReleaseTest):
    def test_valid_stats_ignore_unrelated_frontend_backend_and_other_proxy(self):
        text = stats_csv(self.rows) + (
            "app,FRONTEND,12,0,OPEN\napp,BACKEND,12,0,UP\nother,blue,99,0,UP\n"
        )
        self.assertEqual(release.parse_stats(text), self.rows)

    def test_malformed_socket_stats_fail_closed_without_stopping_any_container(self):
        valid = stats_csv(self.rows)
        malformed = (
            "", "permission denied\n", valid.removeprefix("# "),
            "# pxname,svname,scur,qcur,status\n",
            "# pxname,svname,scur,qcur,status\napp,blue,0,0,UP\n",
            valid.replace("app,green,0,0,MAINT", "app,green,invalid,0,MAINT"),
            valid.replace("app,green,0,0,MAINT", "app,green,0,,MAINT"),
            valid.replace("app,green,0,0,MAINT", "app,green,0"),
            valid.replace("app,green", "other,green"),
        )
        for index, text in enumerate(malformed):
            with self.subTest(case=index):
                self.use_socket_reply(text.encode("utf-8"))
                with self.assertRaises(release.ReleaseError):
                    self.app.drain(0)
                self.assertTrue(all(item["Running"] for item in self.containers.values()))
        self.docker.assert_not_called()

    def test_duplicate_server_rows_cannot_hide_a_live_connection_and_allow_stop(self):
        text = stats_csv({"blue": counters("UP"), "green": counters("DRAIN", sessions=1)})
        text += "app,green,0,0,DRAIN\n"
        self.use_socket_reply(text.encode("utf-8"))
        with self.assertRaises(release.ReleaseError):
            self.app.drain(0)
        self.docker.assert_not_called()
        self.assertTrue(self.containers["green"]["Running"])

    def test_negative_session_or_queue_counters_are_rejected(self):
        for field in ("sessions", "queued"):
            with self.subTest(field=field):
                rows = copy.deepcopy(self.rows)
                rows["green"][field] = -1
                with self.assertRaises(release.ReleaseError):
                    release.parse_stats(stats_csv(rows))

    def test_missing_or_empty_server_status_is_rejected_by_parser(self):
        valid = stats_csv(self.rows)
        for replacement in ("app,green,0,0", "app,green,0,0,"):
            with self.subTest(row=replacement):
                with self.assertRaises(release.ReleaseError):
                    release.parse_stats(valid.replace("app,green,0,0,MAINT", replacement))

    def test_socket_reads_multiple_chunks_before_parsing(self):
        reply = stats_csv(self.rows).encode("utf-8")
        client = self.use_socket_reply(reply[:9], reply[9:27], reply[27:])
        self.assertEqual(self.app.stats(), self.rows)
        client.settimeout.assert_called_once_with(5)
        client.connect.assert_called_once_with(str(self.root / "run" / "admin.sock"))
        client.sendall.assert_called_once_with(b"show stat\n")
        client.shutdown.assert_called_once_with(release.socket.SHUT_WR)
        self.docker.assert_not_called()

    def test_socket_oserror_retains_containers_and_reports_release_error(self):
        self.control.side_effect = lambda command: release.Release.socket_command(self.app, command)
        self.socket_factory.side_effect = OSError("synthetic socket failure")
        with self.assertRaisesRegex(release.ReleaseError, "applications have been retained"):
            self.app.drain(0)
        self.docker.assert_not_called()
        self.assertTrue(all(item["Running"] for item in self.containers.values()))

    def test_oversized_socket_reply_fails_closed(self):
        # Each chunk respects recv(65536); the bound applies to their total.
        chunk = b"x" * 65536
        self.use_socket_reply(*([chunk] * 30), b"x" * 33921)
        with self.assertRaisesRegex(release.ReleaseError, "exceeded its bound"):
            self.app.drain(0)
        self.docker.assert_not_called()
        self.assertTrue(all(item["Running"] for item in self.containers.values()))


if __name__ == "__main__":
    unittest.main()
