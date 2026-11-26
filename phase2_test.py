"""
Phase 2 acceptance test.

Covers:
  P2.1 SCAN/HSCAN/SSCAN/ZSCAN cursor iteration
  P2.2 Bitmap operations (SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD)
  P2.3 HyperLogLog (PFADD/PFCOUNT/PFMERGE)
  P2.4 INFO + SLOWLOG + MEMORY real backends
  P2.5 Blocking commands (BLPOP timeout + wakeup)
  P2.6 SORT (LIST + BY + GET + STORE + ALPHA)
  P2.7 OBJECT/DEBUG/MONITOR/WAIT stubs
"""
import os
import socket
import subprocess
import sys
import threading
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
        return ("$", d.decode("latin-1"))  # latin-1 round-trips any byte
    if t == b"*":
        n = int(read_line(s))
        if n == -1: return ("*", None)
        return ("*", [reply(s) for _ in range(n)])
    raise AssertionError(t)


def cmd(s, *args):
    s.sendall(encode(*args))
    return reply(s)


def cmd_bytes(s, *args):
    """Encode with raw bytes (for binary safety tests)."""
    out = b"*" + str(len(args)).encode() + b"\r\n"
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    s.sendall(out)
    return reply(s)


def start_server():
    for f in ("dump.rdb", "appendonly.aof"):
        if os.path.exists(f): os.remove(f)
    if os.path.exists("redis.conf"): os.remove("redis.conf")
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


def test_scan():
    section("P2.1 SCAN/HSCAN/SSCAN/ZSCAN")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        results = []

        # Populate 100 keys
        for i in range(100):
            cmd(s, "SET", "scankey:%03d" % i, "v")

        # SCAN cursor=0, COUNT=10, iterate until 0
        cursor = "0"
        seen = set()
        iterations = 0
        while True:
            r = cmd(s, "SCAN", cursor, "COUNT", "10")
            iterations += 1
            cursor = r[1][0][1]
            keys = [k[1] for k in r[1][1][1]]
            seen.update(keys)
            if cursor == "0": break
            if iterations > 30: break
        results.append(check("SCAN visited ~100 keys (got %d)" % len(seen), 90 <= len(seen) <= 100,
                              "iter=%d" % iterations))

        # SCAN with MATCH
        cursor = "0"; matched = set()
        while True:
            r = cmd(s, "SCAN", cursor, "MATCH", "scankey:0*", "COUNT", "100")
            cursor = r[1][0][1]
            matched.update(k[1] for k in r[1][1][1])
            if cursor == "0": break
            if len(matched) > 200: break
        results.append(check("SCAN MATCH scankey:0*", all(m.startswith("scankey:0") for m in matched),
                              "matched=%d" % len(matched)))

        # HSCAN
        cmd(s, "HSET", "h", *(sum(([f, str(v)] for f, v in [(("f%d" % i), i) for i in range(50)]), [])))
        cursor = "0"; hf = set()
        while True:
            r = cmd(s, "HSCAN", "h", cursor, "COUNT", "8")
            cursor = r[1][0][1]
            flat = [k[1] for k in r[1][1][1]]
            for i in range(0, len(flat), 2): hf.add(flat[i])
            if cursor == "0": break
        results.append(check("HSCAN found all 50 fields", len(hf) == 50, "got=%d" % len(hf)))

        # SSCAN
        cmd(s, "SADD", "s", *("m%d" % i for i in range(30)))
        cursor = "0"; sm = set()
        while True:
            r = cmd(s, "SSCAN", "s", cursor, "COUNT", "5")
            cursor = r[1][0][1]
            sm.update(k[1] for k in r[1][1][1])
            if cursor == "0": break
        results.append(check("SSCAN found all 30 members", len(sm) == 30))

        # ZSCAN
        args = []
        for i in range(20): args += [str(i), "z%d" % i]
        cmd(s, "ZADD", "z", *args)
        cursor = "0"; zm = set()
        while True:
            r = cmd(s, "ZSCAN", "z", cursor, "COUNT", "4")
            cursor = r[1][0][1]
            flat = [k[1] for k in r[1][1][1]]
            for i in range(0, len(flat), 2): zm.add(flat[i])
            if cursor == "0": break
        results.append(check("ZSCAN found all 20 members", len(zm) == 20))

        s.close()
        return all(results)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_bitmaps():
    section("P2.2 Bitmap operations")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        cmd_bytes(s, "SET", "bm", b"abc")
        # 'a' = 0x61 = 0b01100001. MSB-first bit positions: 0=0,1=1,2=1,3=0,4=0,5=0,6=0,7=1
        r.append(check("GETBIT bm 0 (=0)", cmd(s, "GETBIT", "bm", "0")[1] == 0))
        r.append(check("GETBIT bm 1 (=1)", cmd(s, "GETBIT", "bm", "1")[1] == 1))
        r.append(check("GETBIT bm 2 (=1)", cmd(s, "GETBIT", "bm", "2")[1] == 1))
        r.append(check("GETBIT bm 7 (=1)", cmd(s, "GETBIT", "bm", "7")[1] == 1))
        r.append(check("GETBIT bm 100 (oob=0)", cmd(s, "GETBIT", "bm", "100")[1] == 0))

        cmd(s, "SETBIT", "bm2", "7", "1")
        r.append(check("SETBIT bm2 7 1 grows to 1 byte", cmd(s, "STRLEN", "bm2")[1] == 1))
        r.append(check("GETBIT bm2 7", cmd(s, "GETBIT", "bm2", "7")[1] == 1))

        cmd_bytes(s, "SET", "bm3", b"aa")  # 0x61 0x61 = popcount 6
        r.append(check("BITCOUNT bm3 = 6", cmd(s, "BITCOUNT", "bm3")[1] == 6))
        r.append(check("BITCOUNT bm3 0 0 = 3", cmd(s, "BITCOUNT", "bm3", "0", "0")[1] == 3))

        cmd_bytes(s, "SET", "b1", b"\xff")
        cmd_bytes(s, "SET", "b2", b"\x0f")
        cmd(s, "BITOP", "AND", "dst", "b1", "b2")
        r.append(check("BITOP AND \\xff & \\x0f = \\x0f", cmd(s, "GET", "dst")[1].encode("latin-1") == b"\x0f"))
        cmd(s, "BITOP", "OR", "dst", "b1", "b2")
        r.append(check("BITOP OR  \\xff | \\x0f = \\xff", cmd(s, "GET", "dst")[1].encode("latin-1") == b"\xff"))
        cmd(s, "BITOP", "XOR", "dst", "b1", "b2")
        r.append(check("BITOP XOR \\xff ^ \\x0f = \\xf0", cmd(s, "GET", "dst")[1].encode("latin-1") == b"\xf0"))

        # BITFIELD: u8 at offset 0 = 0x41 = 65
        cmd(s, "SET", "bf", "\x41")
        r.append(check("BITFIELD GET u8 0", cmd(s, "BITFIELD", "bf", "GET", "u8", "0") == ("*", [(":",65)])))
        r.append(check("BITFIELD SET u8 0 200",
                       cmd(s, "BITFIELD", "bf", "SET", "u8", "0", "200") == ("*", [(":",65)])))
        r.append(check("BITFIELD INCRBY u8 0 55 = 255",
                       cmd(s, "BITFIELD", "bf", "INCRBY", "u8", "0", "55") == ("*", [(":",255)])))
        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_hll():
    section("P2.3 HyperLogLog")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        cmd(s, "DEL", "hll1")
        for i in range(1000):
            cmd(s, "PFADD", "hll1", "elem-%d" % i)
        est = cmd(s, "PFCOUNT", "hll1")[1]
        r.append(check("PFCOUNT ~1000 (got %d)" % est, 800 <= est <= 1200))

        cmd(s, "PFADD", "hll2", *("x-%d" % i for i in range(500)))
        cmd(s, "PFMERGE", "merged", "hll1", "hll2")
        merged_est = cmd(s, "PFCOUNT", "merged")[1]
        # Union of 1000 + 500 distinct = 1500
        r.append(check("PFMERGE ~1500 (got %d)" % merged_est, 1200 <= merged_est <= 1800))

        # Same-element set should be exactly 1
        cmd(s, "DEL", "hll3")
        for i in range(100): cmd(s, "PFADD", "hll3", "same")
        r.append(check("PFCOUNT of all-same = 1", cmd(s, "PFCOUNT", "hll3")[1] == 1))
        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_info_slowlog():
    section("P2.4 INFO + SLOWLOG + MEMORY")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        info = cmd(s, "INFO")[1]
        r.append(check("INFO contains server section", "# Server" in info))
        r.append(check("INFO contains redis_version", "redis_version:" in info))
        r.append(check("INFO contains stats section", "# Stats" in info))
        r.append(check("INFO contains total_commands_processed", "total_commands_processed:" in info))

        # Trigger some commands then check stats counter
        for i in range(100): cmd(s, "SET", "k%d" % i, "v")
        info = cmd(s, "INFO")[1]
        m = [l for l in info.split("\r\n") if l.startswith("total_commands_processed")]
        if m:
            val = int(m[0].split(":")[1])
            r.append(check("INFO commands counter increased", val > 100, "got=%d" % val))

        # SLOWLOG LEN/RESET
        sl = cmd(s, "SLOWLOG", "LEN")[1]
        r.append(check("SLOWLOG LEN returns int", isinstance(sl, int)))
        cmd(s, "SLOWLOG", "RESET")
        r.append(check("SLOWLOG RESET returns OK", cmd(s, "SLOWLOG", "RESET")[0] == "+"))

        # MEMORY USAGE
        cmd(s, "SET", "memkey", "x" * 100)
        mu = cmd(s, "MEMORY", "USAGE", "memkey")[1]
        r.append(check("MEMORY USAGE on 100-byte string > 100", mu > 100, "got=%d" % mu))
        r.append(check("MEMORY USAGE on missing key = nil", cmd(s, "MEMORY", "USAGE", "nope")[0] == "$"))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_blocking():
    section("P2.5 BLPOP timeout + wakeup")
    proc = start_server()
    try:
        s1 = socket.create_connection((HOST, PORT), timeout=5)

        # 1. Timeout fires
        t0 = time.time()
        r = cmd(s1, "BLPOP", "empty", "1.0")  # 1 second timeout
        elapsed = time.time() - t0
        timeout_ok = (r[0] == "$" and r[1] is None) and 0.8 <= elapsed <= 2.5
        print("  BLPOP timeout: %s in %.2fs" % (r, elapsed))

        # 2. Wakeup via push from another client
        s1.settimeout(5)
        result = {}
        def blocking_thread():
            result["r"] = cmd(s1, "BLPOP", "wakey", "5.0")
            result["t"] = time.time()
        t = threading.Thread(target=blocking_thread)
        t.start()
        time.sleep(0.5)  # ensure BLPOP is parked
        s2 = socket.create_connection((HOST, PORT), timeout=5)
        cmd(s2, "RPUSH", "wakey", "hello")
        t.join(timeout=3)
        wakeup_ok = (not t.is_alive()) and result.get("r") == ("*", [("$","wakey"),("$","hello")])
        print("  BLPOP wakeup:", result.get("r"))
        s2.close()
        s1.close()
        return timeout_ok and wakeup_ok
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_sort_object_debug():
    section("P2.6/P2.7 SORT + OBJECT + DEBUG")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # SORT numeric list ASC
        cmd(s, "RPUSH", "nums", "3", "1", "4", "1", "5", "9", "2", "6")
        asc = cmd(s, "SORT", "nums", "ASC")
        r.append(check("SORT nums ASC", asc ==
                       ("*", [("$","1"),("$","1"),("$","2"),("$","3"),("$","4"),("$","5"),("$","6"),("$","9")])))
        desc = cmd(s, "SORT", "nums", "DESC")
        r.append(check("SORT nums DESC",
                       desc[0] == "*" and len(desc[1]) == 8 and desc[1][0] == ("$","9")))

        # SORT ALPHA on strings
        cmd(s, "DEL", "strs")
        cmd(s, "RPUSH", "strs", "banana", "apple", "cherry")
        alpha = cmd(s, "SORT", "strs", "ALPHA")
        r.append(check("SORT strs ALPHA",
                       alpha == ("*", [("$","apple"),("$","banana"),("$","cherry")])))

        # SORT BY external key (numeric)
        cmd(s, "SET", "w_banana", "10")
        cmd(s, "SET", "w_apple", "1")
        cmd(s, "SET", "w_cherry", "5")
        byr = cmd(s, "SORT", "strs", "BY", "w_*")
        r.append(check("SORT strs BY w_*",
                       byr == ("*", [("$","apple"),("$","cherry"),("$","banana")])))

        # SORT STORE
        r.append(check("SORT nums STORE out_list",
                       cmd(s, "SORT", "nums", "ASC", "STORE", "out_list")[0] == ":"))
        r.append(check("SORT STORE result len", cmd(s, "LLEN", "out_list")[1] == 8))

        # OBJECT ENCODING
        cmd(s, "SET", "ok", "hello")
        enc = cmd(s, "OBJECT", "ENCODING", "ok")
        r.append(check("OBJECT ENCODING returns a string", enc[0] == "$" and enc[1] in ("raw","embstr","int")))
        r.append(check("OBJECT ENCODING on missing key = nil",
                       cmd(s, "OBJECT", "ENCODING", "nope")[0] == "$"))

        # DEBUG OBJECT
        r.append(check("DEBUG OBJECT returns status",
                       cmd(s, "DEBUG", "OBJECT", "ok")[0] == "+"))

        # MONITOR acks
        r.append(check("MONITOR returns OK", cmd(s, "MONITOR")[0] == "+"))

        # WAIT (single-node always 0)
        r.append(check("WAIT 1 100 returns 0", cmd(s, "WAIT", "1", "100")[1] == 0))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def main():
    results = []
    results.append(("P2.1 SCAN family",     test_scan()))
    results.append(("P2.2 Bitmaps",         test_bitmaps()))
    results.append(("P2.3 HyperLogLog",     test_hll()))
    results.append(("P2.4 INFO/SLOWLOG",    test_info_slowlog()))
    results.append(("P2.5 BLPOP",           test_blocking()))
    results.append(("P2.6/7 SORT/OBJECT",   test_sort_object_debug()))
    print("\n=== Summary ===")
    for name, ok in results:
        print("  %s: %s" % (name, "OK" if ok else "FAIL"))
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
