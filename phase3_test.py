"""
Phase 3 acceptance test — internal encodings.

Validates that OBJECT ENCODING returns the right name for each value shape:
  P3.1 String: int / embstr / raw
  P3.2 Set:    intset (small int-only) → hashtable (on non-int or >512)
  P3.3 Hash:   listpack (small) → hashtable (on grow or long value)
  P3.4 ZSet:   listpack (small) → skiplist (on grow or long value)
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


def encoding_of(s, key):
    r = cmd(s, "OBJECT", "ENCODING", key)
    return r[1] if r[0] == "$" else None


def section(name):
    print("\n=== %s ===" % name)


def test_string_encodings():
    section("P3.1 String encodings")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        cmd(s, "SET", "k_int", "12345")
        r.append(check("numeric string → int", encoding_of(s, "k_int") == "int", "got=%s" % encoding_of(s, "k_int")))

        cmd(s, "SET", "k_neg", "-42")
        r.append(check("negative int → int", encoding_of(s, "k_neg") == "int", "got=%s" % encoding_of(s, "k_neg")))

        cmd(s, "SET", "k_short", "hello")
        r.append(check("short string → embstr", encoding_of(s, "k_short") == "embstr", "got=%s" % encoding_of(s, "k_short")))

        long_val = "x" * 100
        cmd(s, "SET", "k_long", long_val)
        r.append(check("long string → raw", encoding_of(s, "k_long") == "raw", "got=%s" % encoding_of(s, "k_long")))

        # INCR should keep it as int
        cmd(s, "INCR", "k_int")
        r.append(check("after INCR still int", encoding_of(s, "k_int") == "int", "got=%s" % encoding_of(s, "k_int")))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_set_intset():
    section("P3.2 Set: intset → hashtable promotion")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        # All-integer small set → intset
        cmd(s, "SADD", "iset", "1", "2", "3")
        r.append(check("3 ints → intset", encoding_of(s, "iset") == "intset", "got=%s" % encoding_of(s, "iset")))

        # Add non-int member → promote to hashtable
        cmd(s, "SADD", "iset", "foo")
        r.append(check("after non-int add → hashtable", encoding_of(s, "iset") == "hashtable",
                       "got=%s" % encoding_of(s, "iset")))

        # SCARD still works
        r.append(check("SCARD iset = 4", cmd(s, "SCARD", "iset")[1] == 4))
        r.append(check("SISMEMBER iset 2", cmd(s, "SISMEMBER", "iset", "2")[1] == 1))
        r.append(check("SISMEMBER iset foo", cmd(s, "SISMEMBER", "iset", "foo")[1] == 1))
        r.append(check("SREM iset foo", cmd(s, "SREM", "iset", "foo")[1] == 1))
        r.append(check("SMEMBERS works", cmd(s, "SMEMBERS", "iset")[0] == "*"))

        # Set of > 512 integers → promote to hashtable
        cmd(s, "DEL", "bigiset")
        for i in range(600):
            cmd(s, "SADD", "bigiset", str(i))
        r.append(check("600 ints → hashtable (over threshold)", encoding_of(s, "bigiset") == "hashtable",
                       "got=%s" % encoding_of(s, "bigiset")))
        r.append(check("SCARD bigiset = 600", cmd(s, "SCARD", "bigiset")[1] == 600))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_hash_listpack():
    section("P3.3 Hash: listpack → hashtable promotion")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        cmd(s, "HSET", "h", "f1", "v1", "f2", "v2")
        r.append(check("2 fields → listpack", encoding_of(s, "h") == "listpack", "got=%s" % encoding_of(s, "h")))

        # Functional checks
        r.append(check("HGET h f1", cmd(s, "HGET", "h", "f1")[1] == "v1"))
        r.append(check("HLEN h = 2", cmd(s, "HLEN", "h")[1] == 2))
        r.append(check("HDEL h f1", cmd(s, "HDEL", "h", "f1")[1] == 1))

        # Add 200 fields → promote
        for i in range(200):
            cmd(s, "HSET", "big", "field_%03d" % i, "v")
        r.append(check("200 fields → hashtable", encoding_of(s, "big") == "hashtable",
                       "got=%s" % encoding_of(s, "big")))
        r.append(check("HLEN big = 200", cmd(s, "HLEN", "big")[1] == 200))

        # Add a long value (> 64 bytes) on a fresh hash → promote immediately
        cmd(s, "HSET", "h_longval", "k", "x" * 100)
        r.append(check("long value → hashtable", encoding_of(s, "h_longval") == "hashtable",
                       "got=%s" % encoding_of(s, "h_longval")))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def test_zset_listpack_skiplist():
    section("P3.4 ZSet: listpack → skiplist promotion")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        r = []

        cmd(s, "ZADD", "z", "1", "a", "2", "b", "3", "c")
        r.append(check("3 members → listpack", encoding_of(s, "z") == "listpack", "got=%s" % encoding_of(s, "z")))

        # Functional checks
        r.append(check("ZCARD z = 3", cmd(s, "ZCARD", "z")[1] == 3))
        r.append(check("ZSCORE z b = 2", cmd(s, "ZSCORE", "z", "b")[1] == "2"))
        r.append(check("ZRANGE z 0 -1 returns sorted",
                       cmd(s, "ZRANGE", "z", 0, -1) ==
                       ("*", [("$","a"),("$","b"),("$","c")])))

        # Add 200 members → promote to skiplist
        for i in range(200):
            cmd(s, "ZADD", "big", str(i), "m%03d" % i)
        r.append(check("200 members → skiplist", encoding_of(s, "big") == "skiplist",
                       "got=%s" % encoding_of(s, "big")))
        r.append(check("ZCARD big = 200", cmd(s, "ZCARD", "big")[1] == 200))

        # Long member (> 64 bytes) → promote
        cmd(s, "ZADD", "z_longmember", "1", "x" * 100)
        r.append(check("long member → skiplist", encoding_of(s, "z_longmember") == "skiplist",
                       "got=%s" % encoding_of(s, "z_longmember")))

        s.close()
        return all(r)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


def main():
    results = []
    results.append(("P3.1 String encodings", test_string_encodings()))
    results.append(("P3.2 Set intset",      test_set_intset()))
    results.append(("P3.3 Hash listpack",   test_hash_listpack()))
    results.append(("P3.4 ZSet listpack",   test_zset_listpack_skiplist()))
    print("\n=== Summary ===")
    for name, ok in results:
        print("  %s: %s" % (name, "OK" if ok else "FAIL"))
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
