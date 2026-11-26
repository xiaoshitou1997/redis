"""
Phase 7 acceptance test — RESP3 wire types + Stream XCLAIM/XAUTOCLAIM + perf sanity.
"""
import os, socket, subprocess, sys, time

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
        c = s.recv(1); buf += c
        if buf[-2:] == b"\r\n": return bytes(buf[:-2])


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
    if t == b"%":
        n = int(read_line(s))
        # Map: read n key-value pairs
        return ("%", dict((reply(s)[1], reply(s)[1]) for _ in range(n)))
    if t == b"~":
        n = int(read_line(s))
        return ("~", [reply(s) for _ in range(n)])
    if t == b"#":
        c = s.recv(1); read_line(s)
        return ("#", c == b"t")
    if t == b",":
        return (",", read_line(s).decode())
    raise AssertionError(t)


def cmd(s, *args):
    s.sendall(encode(*args))
    return reply(s)


def start_server(conf=None):
    if conf:
        with open("redis.conf", "w") as f: f.write(conf)
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
            with socket.create_connection((HOST, PORT), timeout=1): return proc
        except OSError: time.sleep(0.25)
    proc.terminate()
    print(open("server.log").read())
    raise RuntimeError("server failed to start")


def check(label, cond, extra=""):
    print(("OK   " if cond else "FAIL ") + label + (extra and " " + extra))
    return cond


def section(name):
    print("\n=== %s ===" % name)


def test_resp3_wire_types():
    section("P7.1 RESP3 wire types")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # HELLO 3 — should return a map starting with '%'
        h3 = cmd(s, "HELLO", "3")
        r.append(check("HELLO 3 returns RESP3 map", h3[0] == "%", "got=%r" % (h3,)))
        if h3[0] == "%":
            r.append(check("HELLO 3 map has server", h3[1].get("server") == "redis"))
            r.append(check("HELLO 3 map has proto=3", h3[1].get("proto") == 3))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_stream_xclaim():
    section("P7.2 XCLAIM/XAUTOCLAIM")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # Set up stream + group + 1 entry
        cmd(s, "XADD", "s", "*", "k", "v1")
        cmd(s, "XADD", "s", "*", "k", "v2")
        cmd(s, "XADD", "s", "*", "k", "v3")
        cmd(s, "XGROUP", "CREATE", "s", "g1", "0")

        # Read all 3 entries into consumer c1
        read1 = cmd(s, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "10", "STREAMS", "s", ">")
        # read1 = ('*', [ ('*', [('$', 's'), ('*', [e1, e2, e3])]) ])
        # read1[1][0] = the (key, entries-array-reply) tuple
        # read1[1][0][1] = list of reply tuples: [('s' bulk), (entries-array-reply)]
        entries_array_reply = read1[1][0][1][1]  # the ('*', [e1, e2, e3])
        entries_list = entries_array_reply[1]    # the [e1, e2, e3]
        r.append(check("XREADGROUP delivered 3 entries",
                       read1[0] == "*" and len(entries_list) == 3))

        # Get the first id: entry = ('*', [('$', id), ('*', [fields])])
        first_id = entries_list[0][1][0][1]

        # XCLAIM: claim first_id with min_idle_time=0 to consumer c2
        claim = cmd(s, "XCLAIM", "s", "g1", "c2", "0", first_id)
        r.append(check("XCLAIM returns array", claim[0] == "*"))
        r.append(check("XCLAIM delivers 1 entry", len(claim[1]) == 1))

        # XCLAIM with JUSTID
        claim_justid = cmd(s, "XCLAIM", "s", "g1", "c2", "0", first_id, "JUSTID")
        r.append(check("XCLAIM JUSTID returns array of ids", claim_justid[0] == "*" and len(claim_justid[1]) == 1))

        # XAUTOCLAIM: scan PEL starting from "0" with min_idle_time=0
        ac = cmd(s, "XAUTOCLAIM", "s", "g1", "c3", "0", "0", "COUNT", "10")
        r.append(check("XAUTOCLAIM returns 3-element array", ac[0] == "*" and len(ac[1]) == 3))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_perf_pooled_path():
    section("P7.4 Perf sanity with pooled reply buffer")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        for i in range(1000): cmd(s, "SET", "warm%d" % i, "v")

        N = 50000
        pipeline = b""
        for i in range(N):
            pipeline += encode("SET", "p7:%d" % i, "value")

        t0 = time.time()
        CHUNK = 65536
        sent = 0
        while sent < len(pipeline):
            end = min(sent + CHUNK, len(pipeline))
            s.sendall(pipeline[sent:end]); sent = end

        received = 0
        s.setblocking(False)
        deadline = time.time() + 30
        while received < N and time.time() < deadline:
            try:
                byte = s.recv(1)
                if byte == b"+":
                    while True:
                        c = s.recv(1)
                        if c == b"\n": break
                    received += 1
            except BlockingIOError:
                time.sleep(0.001)
        elapsed = time.time() - t0
        s.setblocking(True)
        rate = received / elapsed
        ok = received >= N * 0.95 and rate >= 50000
        check("Throughput >= 50k ops/sec with pooled reply buffer", ok, "(%.0f ops/sec, %d/%d)" % (rate, received, N))
        s.close()
        return ok
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def main():
    results = []
    results.append(("P7.1 RESP3 wire types", test_resp3_wire_types()))
    results.append(("P7.2 Stream XCLAIM",   test_stream_xclaim()))
    results.append(("P7.4 Perf pooled",     test_perf_pooled_path()))
    print("\n=== Summary ===")
    for name, ok in results:
        print("  %s: %s" % (name, "OK" if ok else "FAIL"))
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
