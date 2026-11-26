"""
Phase 4 acceptance test — Streams, GEO, Lua, HELLO.

P4.1: XADD / XLEN / XRANGE / XREAD / XDEL / XGROUP / XREADGROUP / XACK / XPENDING / XTRIM
P4.2: GEOADD / GEODIST / GEOHASH / GEOPOS / GEOSEARCH
P4.3: EVAL / EVALSHA / SCRIPT LOAD/EXISTS/FLUSH + redis.call + KEYS/ARGV
P4.4: HELLO (proto 2/3) + AUTH via HELLO
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
        return ("$", d.decode(errors="replace"))
    if t == b"*":
        n = int(read_line(s))
        return ("*", [reply(s) for _ in range(n)])
    raise AssertionError(t)


def cmd(s, *args):
    s.sendall(encode(*args))
    return reply(s)


def start_server():
    for f in ("dump.rdb", "appendonly.aof", "redis.conf"):
        if os.path.exists(f): os.remove(f)
    proc = subprocess.Popen(
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


def test_stream():
    section("P4.1 Stream")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # XADD with auto-id
        add1 = cmd(s, "XADD", "mystream", "*", "field1", "v1", "field2", "v2")
        r.append(check("XADD auto-id returns bulk string", add1[0] == "$" and "-" in add1[1], "got=%s" % (add1,)))
        first_id = add1[1]
        add2 = cmd(s, "XADD", "mystream", "*", "f", "v2")
        r.append(check("XADD 2nd entry returns higher id", add2[1] > first_id, "ids=%s < %s" % (first_id, add2[1])))

        # XADD with explicit id (must be greater than auto-generated).
        # Use a sufficiently large offset so the explicit id is always greater
        # than any subsequent wall-clock-based auto-id we'll generate.
        first_ms = int(first_id.split("-")[0])
        explicit_id = str(first_ms + 100000) + "-0"
        explicit_reply = cmd(s, "XADD", "mystream", explicit_id, "f", "explicit")
        r.append(check("XADD explicit id accepted", explicit_reply[0] == "$", "got=%r" % (explicit_reply,)))
        bad = cmd(s, "XADD", "mystream", "1-0", "f", "older")
        r.append(check("XADD with older id → error", bad[0] == "-", "got=%s" % (bad,)))

        # XLEN — now 3 entries (2 auto-id + 1 explicit)
        xlen3 = cmd(s, "XLEN", "mystream")
        r.append(check("XLEN mystream = 3", xlen3[1] == 3, "got=%r" % (xlen3,)))

        # XRANGE returns all entries currently in the stream.
        rng = cmd(s, "XRANGE", "mystream", "-", "+")
        # rng = ('*', [entry1, entry2, entry3]) — len(rng[1]) is entry count
        r.append(check("XRANGE - + returns >= 3 entries", rng[0] == "*" and len(rng[1]) >= 3,
                       "got=%d entries" % len(rng[1])))

        # XDEL (delete the explicit-id entry)
        before = cmd(s, "XLEN", "mystream")[1]
        delr = cmd(s, "XDEL", "mystream", explicit_id)
        r.append(check("XDEL returns 1", delr[1] == 1))
        r.append(check("XLEN after XDEL", cmd(s, "XLEN", "mystream")[1] == before - 1))

        # XGROUP CREATE + XREADGROUP
        cmd(s, "XGROUP", "CREATE", "mystream", "grp1", "0")
        cmd(s, "XADD", "mystream", "*", "msg", "hello")
        cmd(s, "XADD", "mystream", "*", "msg", "world")
        grp_read = cmd(s, "XREADGROUP", "GROUP", "grp1", "consumer1", "COUNT", "10", "STREAMS", "mystream", ">")
        r.append(check("XREADGROUP returns array", grp_read[0] == "*" and len(grp_read[1]) == 1, "got=%s" % (grp_read,)))

        # XPENDING — group created at 0 so all current entries are pending.
        pend = cmd(s, "XPENDING", "mystream", "grp1")
        # pend = ('*', [(':', count), ('$', min_id), ('$', max_id), ('*', [...])])
        pending_count = pend[1][0][1] if pend[0] == "*" and len(pend[1]) > 0 else 0
        r.append(check("XPENDING summary has pending entries", pending_count >= 2,
                       "got=%r" % (pend,)))

        # XACK all currently-delivered ids.
        delivered_ids = []
        # grp_read = ('*', [ ('*', [('$', key), ('*', [entry1, entry2, ...])]) ])
        # Walk: grp_read[1][0] = (key-array-reply), [1] = inner list,
        #      [1] = entries-array-reply, [1] = entries list
        entries_list = grp_read[1][0][1][1][1]
        for entry_row in entries_list:
            # entry_row = ('*', [('$', id), ('*', [...fields...])])
            delivered_ids.append(entry_row[1][0][1])
        ack_args = ["XACK", "mystream", "grp1"] + delivered_ids
        ack = cmd(s, *ack_args)
        r.append(check("XACK returns %d" % len(delivered_ids), ack[1] == len(delivered_ids), "got=%r" % (ack,)))

        # XTRIM
        cmd(s, "XADD", "trimtest", "*", "x", "1")
        cmd(s, "XADD", "trimtest", "*", "x", "2")
        cmd(s, "XADD", "trimtest", "*", "x", "3")
        cmd(s, "XADD", "trimtest", "*", "x", "4")
        cmd(s, "XADD", "trimtest", "*", "x", "5")
        trimmed = cmd(s, "XTRIM", "trimtest", "MAXLEN", "=", "2")
        r.append(check("XTRIM trims 3", trimmed[1] == 3, "got=%s" % (trimmed,)))
        r.append(check("XLEN trimtest = 2", cmd(s, "XLEN", "trimtest")[1] == 2))

        # XINFO STREAM
        info = cmd(s, "XINFO", "STREAM", "mystream")
        r.append(check("XINFO STREAM returns array", info[0] == "*", "got=%s" % (info,)))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_geo():
    section("P4.2 GEO")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # GEOADD: Beijing, Shanghai, Tokyo
        added = cmd(s, "GEOADD", "cities", "116.40", "39.90", "Beijing",
                                                   "121.47", "31.23", "Shanghai",
                                                   "139.69", "35.69", "Tokyo")
        r.append(check("GEOADD 3 cities → 3", added[1] == 3))

        # GEODIST: Beijing → Shanghai in km (~1067 km)
        dist = cmd(s, "GEODIST", "cities", "Beijing", "Shanghai", "km")
        r.append(check("Beijing-Shanghai distance ~1000-1200 km", dist[0] == "$" and 1000 < float(dist[1]) < 1200,
                       "got=%s" % (dist,)))

        # GEOPOS
        pos = cmd(s, "GEOPOS", "cities", "Beijing")
        # pos = ('*', [ ('*', [('$', lon), ('$', lat)]) ])
        lon = float(pos[1][0][1][0][1])
        lat = float(pos[1][0][1][1][1])
        r.append(check("GEOPOS Beijing lon ~116.40", 116.0 < lon < 117.0, "lon=%s" % lon))
        r.append(check("GEOPOS Beijing lat ~39.90", 39.0 < lat < 41.0, "lat=%s" % lat))

        # GEOHASH (11 char string)
        hsh = cmd(s, "GEOHASH", "cities", "Beijing")
        # hsh = ('*', [('$', 'xxxxxxxxxxx')])
        r.append(check("GEOHASH returns 11-char string", hsh[1][0][1] and len(hsh[1][0][1]) == 11,
                       "got=%r" % (hsh[1][0][1],)))

        # GEOSEARCH BYRADIUS — find Chinese cities within 2000 km of Beijing
        found = cmd(s, "GEOSEARCH", "cities", "FROMMEMBER", "Beijing", "BYRADIUS", "2000", "km")
        # found = ('*', [('$', 'Beijing'), ('$', 'Shanghai'), ...])
        members = [e[1] for e in found[1]]
        r.append(check("GEOSEARCH finds Shanghai within 2000km", "Shanghai" in members, "members=%s" % members))
        r.append(check("GEOSEARCH excludes Tokyo (>2000km)", "Tokyo" not in members, "members=%s" % members))

        # GEOSEARCH WITHDIST WITHCOORD
        wd = cmd(s, "GEOSEARCH", "cities", "FROMMEMBER", "Beijing", "BYRADIUS", "2000", "km", "WITHDIST", "WITHCOORD")
        # wd = ('*', [ ('*', [('$', member), ('$', dist), ('*', [lon, lat])]) ])
        r.append(check("GEOSEARCH WITHDIST WITHCOORD returns enriched rows",
                       wd[0] == "*" and len(wd[1]) >= 1 and wd[1][0][0] == "*",
                       "got=%r" % (wd,)))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_lua():
    section("P4.3 Lua scripting")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # EVAL: return literal
        r.append(check("EVAL return 42",
                       cmd(s, "EVAL", "return 42", "0") == (":", 42)))

        # EVAL: KEYS / ARGV access
        r.append(check("EVAL KEYS[1]",
                       cmd(s, "EVAL", "return KEYS[1]", "1", "mykey") == ("$", "mykey")))
        r.append(check("EVAL ARGV[1]",
                       cmd(s, "EVAL", "return ARGV[1]", "0", "hello") == ("$", "hello")))

        # EVAL: redis.call() — SET returns status "OK" (simple string)
        r.append(check("EVAL redis.call SET",
                       cmd(s, "EVAL", "return redis.call('SET', KEYS[1], 'lua_val')", "1", "scripted_key") == ("+", "OK")))
        r.append(check("EVAL redis.call GET",
                       cmd(s, "EVAL", "return redis.call('GET', KEYS[1])", "1", "scripted_key") == ("$", "lua_val")))

        # EVAL: redis.call INCR + math
        cmd(s, "SET", "cnt", "10")
        r.append(check("EVAL INCR math",
                       cmd(s, "EVAL", "return redis.call('GET', KEYS[1]) + 5", "1", "cnt") == (":", 15)))

        # EVALSHA / SCRIPT LOAD
        load = cmd(s, "SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])")
        sha = load[1]
        r.append(check("SCRIPT LOAD returns SHA1 (40 hex)", load[0] == "$" and len(sha) == 40))

        ex = cmd(s, "SCRIPT", "EXISTS", sha, "00" * 20)
        # ex = ('*', [(':', 1), (':', 0)])
        r.append(check("SCRIPT EXISTS [sha, fake]",
                       ex[0] == "*" and len(ex[1]) == 2 and ex[1][0] == (":", 1) and ex[1][1] == (":", 0),
                       "got=%r" % (ex,)))

        ev = cmd(s, "EVALSHA", sha, "1", "cnt")
        # The script does redis.call('GET', KEYS[1]) + 5 — returns a number.
        r.append(check("EVALSHA on loaded script returns number", ev[0] == ":" and isinstance(ev[1], int),
                       "got=%r" % (ev,)))

        # SCRIPT FLUSH
        cmd(s, "SCRIPT", "FLUSH")
        ex2 = cmd(s, "SCRIPT", "EXISTS", sha)
        r.append(check("After FLUSH script gone", ex2 == ("*", [(": ", 0)]) or
                       (ex2[0] == "*" and ex2[1] == [(": ", 0)]) or
                       (ex2[0] == "*" and len(ex2[1]) == 1 and ex2[1][0] == (":", 0)),
                       "got=%r" % (ex2,)))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_hello():
    section("P4.4 HELLO")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # HELLO 2
        h2 = cmd(s, "HELLO", "2")
        # h2 = ('*', [('$', 'server'), ('$', 'redis'), ('$', 'version'), ('$', '8.0.0-phase4'), ...])
        r.append(check("HELLO 2 returns array", h2[0] == "*", "got=%r" % (h2,)))
        # Extract alternating (key, value) pairs.
        flat_values = []
        for i, kv in enumerate(h2[1]):
            if i % 2 == 1:  # value position
                flat_values.append(kv[1] if kv[0] == "$" else str(kv[1]))
        r.append(check("HELLO 2 contains 'redis' and '8.0.0'",
                       "redis" in flat_values and any("8.0.0" in str(v) for v in flat_values),
                       "values=%s" % flat_values))

        # HELLO ? returns array
        hq = cmd(s, "HELLO", "?")
        r.append(check("HELLO ? returns array", hq[0] == "*"))

        # HELLO with unsupported proto
        bad = cmd(s, "HELLO", "99")
        r.append(check("HELLO 99 → NOPROTO error", bad[0] == "-" and "NOPROTO" in bad[1], "got=%s" % (bad,)))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def main():
    results = []
    results.append(("P4.1 Stream",    test_stream()))
    results.append(("P4.2 GEO",       test_geo()))
    results.append(("P4.3 Lua",       test_lua()))
    results.append(("P4.4 HELLO",     test_hello()))
    print("\n=== Summary ===")
    for name, ok in results:
        print("  %s: %s" % (name, "OK" if ok else "FAIL"))
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
