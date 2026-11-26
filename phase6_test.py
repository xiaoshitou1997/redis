"""
Phase 6 acceptance test — replication + cluster MOVED + perf sanity.

P6.1: SLAVEOF host port / SLAVEOF NO ONE works (role switch only — full
      network replication requires multi-process infra).
P6.2: Cluster MOVED redirect (simulated by enabling cluster + CLUSTER KEYSLOT).
P6.3: Throughput sanity (>= 10k SET/GET ops/sec on localhost).
"""
import os
import socket
import subprocess
import sys
import time

HOST, PORT = "127.0.0.1", 6379
JAR = "target/redis-1.0.0-jar-with-dependencies.jar"


def encode(*args):
    out = b"*" + str(len(args)).encode() + b"\r\n"
    for a in args:
        a = a.encode() if isinstance(a, str) else str(a).encode()
        out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    return out


def read_line(s):
    buf = bytearray()
    while True:
        c = s.recv(1)
        buf += c
        if buf[-2:] == b"\r\n":
            return bytes(buf[:-2])


def reply(s):
    t = s.recv(1)
    if t == b"+": return ("+", read_line(s).decode())
    if t == b"-": return ("-", read_line(s).decode())
    if t == b":": return (":", int(read_line(s)))
    if t == b"$":
        n = int(read_line(s))
        if n == -1: return ("$", None)
        d = b""
        while len(d) < n: d += s.recv(n - len(d))
        s.recv(2)
        return ("$", d.decode("latin-1"))
    if t == b"*":
        n = int(read_line(s))
        return ("*", [reply(s) for _ in range(n)])
    raise AssertionError(t)


def cmd(s, *args):
    s.sendall(encode(*args))
    return reply(s)


def start_server(conf=None):
    if conf:
        with open("redis.conf", "w") as f:
            f.write(conf)
    elif os.path.exists("redis.conf"):
        os.remove("redis.conf")
    for f in ("dump.rdb", "appendonly.aof"):
        if os.path.exists(f): os.remove(f)
    proc = subprocess.Popen(
        ["java", "-Dfile.encoding=UTF-8", "-jar", JAR, "redis.conf"] if conf else
        ["java", "-Dfile.encoding=UTF-8", "-jar", JAR],
        stdout=open("server.log", "w"), stderr=subprocess.STDOUT,
    )
    for _ in range(80):
        try:
            with socket.create_connection((HOST, PORT), timeout=1):
                return proc
        except OSError:
            time.sleep(0.25)
    proc.terminate()
    print(open("server.log").read())
    raise RuntimeError("server failed to start")


def check(label, cond, extra=""):
    print(("OK   " if cond else "FAIL ") + label + (extra and " " + extra))
    return cond


def section(name):
    print("\n=== %s ===" % name)


def test_replication_role():
    section("P6.1 SLAVEOF role switch")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # INFO replication — initially master
        info = cmd(s, "INFO", "replication")
        info_str = info[1] if info[0] == "$" else ""
        r.append(check("Initial role is master", "role:master" in info_str,
                       "got=%r" % (info_str[:60],)))

        # SLAVEOF NO ONE — should still be master
        noone = cmd(s, "SLAVEOF", "NO", "ONE")
        r.append(check("SLAVEOF NO ONE → OK", noone[0] == "+"))

        # SLAVEOF localhost 9999 — switch to replica role (no actual master will respond)
        slaveof = cmd(s, "SLAVEOF", "127.0.0.1", "9999")
        r.append(check("SLAVEOF host port → OK", slaveof[0] == "+"))

        # INFO replication — now role:slave
        info2 = cmd(s, "INFO", "replication")
        info2_str = info2[1] if info2[0] == "$" else ""
        r.append(check("After SLAVEOF, role is slave", "role:slave" in info2_str,
                       "got=%r" % (info2_str[:80],)))
        r.append(check("slave has master_host", "master_host:127.0.0.1" in info2_str))
        r.append(check("slave has master_port", "master_port:9999" in info2_str))

        # REPLICAOF alias works the same
        cmd(s, "REPLICAOF", "NO", "ONE")
        info3 = cmd(s, "INFO", "replication")
        info3_str = info3[1] if info3[0] == "$" else ""
        r.append(check("After REPLICAOF NO ONE, role:master", "role:master" in info3_str))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_cluster_moved():
    section("P6.2 Cluster MOVED redirect")
    # Enable cluster mode
    proc = start_server("cluster-enabled yes\n")
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # CLUSTER INFO should show cluster_enabled:1
        info = cmd(s, "CLUSTER", "INFO")
        info_str = info[1] if info[0] == "$" else ""
        r.append(check("CLUSTER INFO shows enabled", "cluster_enabled:1" in info_str,
                       "got=%r" % (info_str[:80],)))

        # CLUSTER KEYSLOT for a known key — sanity check
        slot = cmd(s, "CLUSTER", "KEYSLOT", "mykey")
        r.append(check("CLUSTER KEYSLOT returns integer", slot[0] == ":"))

        # SET a key — since we have no slots assigned, expect MOVED redirect
        setr = cmd(s, "SET", "mykey", "v")
        # In a single-node cluster with no slots explicitly assigned, the owner
        # is null — we either accept (treat self as owner) or MOVED to "0.0.0.0:0".
        # Phase-6 behavior: MOVED with empty address when no owner.
        r.append(check("SET in unconfigured cluster → MOVED or accept",
                       setr[0] == "-" and "MOVED" in setr[1],
                       "got=%r" % (setr,)))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_perf_throughput():
    section("P6.3 Throughput sanity (>= 10k ops/sec)")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)

        # Warmup
        for i in range(1000):
            cmd(s, "SET", "warm%d" % i, "v")

        # Pipelined SET (we don't wait for replies; just count what we send)
        # Single-threaded dispatcher + Netty NIO + Phase 6 pooling should
        # comfortably exceed 10k SET/sec on localhost.
        N = 50000
        # Build all commands up-front (no allocation in the timing loop).
        pipeline = b""
        for i in range(N):
            pipeline += encode("SET", "perf:%d" % i, "value")

        t0 = time.time()
        # Send in 64KB chunks to avoid socket buffer pressure.
        CHUNK = 65536
        sent = 0
        while sent < len(pipeline):
            end = min(sent + CHUNK, len(pipeline))
            s.sendall(pipeline[sent:end])
            sent = end
        # Drain replies
        received = 0
        s.setblocking(False)
        deadline = time.time() + 30  # 30s budget
        while received < N and time.time() < deadline:
            try:
                byte = s.recv(1)
                if byte == b"+":
                    # Read until \r\n
                    while True:
                        c = s.recv(1)
                        if c == b"\n":
                            break
                    received += 1
            except BlockingIOError:
                time.sleep(0.001)
        elapsed = time.time() - t0
        s.setblocking(True)
        rate = received / elapsed
        print("  %d/%d replies in %.2fs = %.0f ops/sec" % (received, N, elapsed, rate))
        ok = received >= N * 0.95 and rate >= 10000
        check("Throughput >= 10k ops/sec", ok, "(got %.0f)" % rate)

        s.close()
        return ok
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def main():
    results = []
    results.append(("P6.1 Replication role", test_replication_role()))
    results.append(("P6.2 Cluster MOVED",   test_cluster_moved()))
    results.append(("P6.3 Throughput",      test_perf_throughput()))
    print("\n=== Summary ===")
    for name, ok in results:
        print("  %s: %s" % (name, "OK" if ok else "FAIL"))
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
