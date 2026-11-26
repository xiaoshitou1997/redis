"""
Smoke test for the Redis server (Phase 0).

Starts the server as a subprocess, runs a battery of RESP commands, and
verifies the responses. Exits with status 0 on success, 1 on failure.
"""
import os
import re
import socket
import subprocess
import sys
import time

HOST = "127.0.0.1"
PORT = 6379
JAR = "target/redis-1.0.0-jar-with-dependencies.jar"
TIMEOUT = 30.0


def encode_resp(*args):
    out = bytearray()
    out += b"*" + str(len(args)).encode() + b"\r\n"
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        elif isinstance(a, int):
            a = str(a).encode()
        out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    return bytes(out)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("socket closed while expecting %d bytes" % n)
        buf += chunk
    return buf


def read_line(sock):
    buf = bytearray()
    while True:
        c = recv_exact(sock, 1)
        buf += c
        if buf[-2:] == b"\r\n":
            return bytes(buf[:-2])


def read_reply(sock):
    typ = recv_exact(sock, 1)
    if typ == b"+":
        return ("simple", read_line(sock).decode())
    if typ == b"-":
        return ("error",  read_line(sock).decode())
    if typ == b":":
        return ("integer", int(read_line(sock)))
    if typ == b"$":
        n = int(read_line(sock))
        if n == -1:
            return ("bulk", None)
        data = recv_exact(sock, n)
        crlf = recv_exact(sock, 2)
        assert crlf == b"\r\n", "expected CRLF after bulk"
        return ("bulk", data.decode(errors="replace"))
    if typ == b"*":
        n = int(read_line(sock))
        if n == -1:
            return ("array", None)
        return ("array", [read_reply(sock) for _ in range(n)])
    raise AssertionError("unknown reply type: %r" % typ)


def send(sock, *args):
    sock.sendall(encode_resp(*args))
    return read_reply(sock)


def assert_eq(actual, expected, label):
    if actual != expected:
        print("FAIL  %-40s got=%r expected=%r" % (label, actual, expected))
        return False
    print("OK    %s -> %r" % (label, actual))
    return True


def main():
    # Clean any stale persistence so the test starts fresh
    for f in ("dump.rdb", "appendonly.aof"):
        if os.path.exists(f):
            os.remove(f)

    proc = subprocess.Popen(
        ["java", "-Dfile.encoding=UTF-8", "-jar", JAR],
        stdout=open("server.log", "w"),
        stderr=subprocess.STDOUT,
    )
    try:
        # Wait for port
        deadline = time.time() + TIMEOUT
        while time.time() < deadline:
            try:
                with socket.create_connection((HOST, PORT), timeout=1):
                    break
            except OSError:
                time.sleep(0.3)
        else:
            print("FAIL  server did not come up")
            print(open("server.log").read())
            return 1

        sock = socket.create_connection((HOST, PORT), timeout=5)
        results = []

        # ===== String =====
        results.append(assert_eq(send(sock, "PING"), ("simple", "PONG"), "PING"))
        results.append(assert_eq(send(sock, "ECHO", "hello"), ("bulk", "hello"), "ECHO"))
        results.append(assert_eq(send(sock, "SET", "k1", "Hello"), ("simple", "OK"), "SET"))
        results.append(assert_eq(send(sock, "GET", "k1"), ("bulk", "Hello"), "GET"))
        results.append(assert_eq(send(sock, "STRLEN", "k1"), ("integer", 5), "STRLEN"))
        results.append(assert_eq(send(sock, "INCR", "counter"), ("integer", 1), "INCR"))
        results.append(assert_eq(send(sock, "INCR", "counter"), ("integer", 2), "INCR again"))
        results.append(assert_eq(send(sock, "INCRBY", "counter", 10), ("integer", 12), "INCRBY"))
        results.append(assert_eq(send(sock, "DECR", "counter"), ("integer", 11), "DECR"))
        results.append(assert_eq(send(sock, "APPEND", "k1", " World"), ("integer", 11), "APPEND"))
        results.append(assert_eq(send(sock, "GET", "k1"), ("bulk", "Hello World"), "GET after APPEND"))
        results.append(assert_eq(send(sock, "GETRANGE", "k1", 0, 4), ("bulk", "Hello"), "GETRANGE"))
        results.append(assert_eq(send(sock, "MSET", "a", "1", "b", "2", "c", "3"), ("simple", "OK"), "MSET"))
        results.append(assert_eq(send(sock, "MGET", "a", "b", "c"), ("array", [("bulk","1"),("bulk","2"),("bulk","3")]), "MGET"))
        results.append(assert_eq(send(sock, "SET", "k1", "newval", "NX"), ("bulk", None), "SET NX on existing key"))

        # ===== Hash =====
        results.append(assert_eq(send(sock, "HSET", "h1", "f1", "v1", "f2", "v2"), ("integer", 2), "HSET 2 new"))
        results.append(assert_eq(send(sock, "HGET", "h1", "f1"), ("bulk", "v1"), "HGET"))
        results.append(assert_eq(send(sock, "HLEN", "h1"), ("integer", 2), "HLEN"))
        results.append(assert_eq(send(sock, "HDEL", "h1", "f1"), ("integer", 1), "HDEL"))
        results.append(assert_eq(send(sock, "HEXISTS", "h1", "f1"), ("integer", 0), "HEXISTS after HDEL"))
        results.append(assert_eq(send(sock, "HINCRBY", "h1", "counter", 5), ("integer", 5), "HINCRBY new field"))

        # ===== List =====
        results.append(assert_eq(send(sock, "RPUSH", "l1", "a", "b", "c"), ("integer", 3), "RPUSH 3"))
        results.append(assert_eq(send(sock, "LRANGE", "l1", 0, -1), ("array", [("bulk","a"),("bulk","b"),("bulk","c")]), "LRANGE"))
        results.append(assert_eq(send(sock, "LPUSH", "l1", "z"), ("integer", 4), "LPUSH 1"))
        results.append(assert_eq(send(sock, "LINDEX", "l1", 0), ("bulk", "z"), "LINDEX 0"))
        results.append(assert_eq(send(sock, "LPOP", "l1"), ("bulk", "z"), "LPOP"))
        results.append(assert_eq(send(sock, "LLEN", "l1"), ("integer", 3), "LLEN"))

        # ===== Set =====
        results.append(assert_eq(send(sock, "SADD", "s1", "m1", "m2", "m3"), ("integer", 3), "SADD 3"))
        results.append(assert_eq(send(sock, "SCARD", "s1"), ("integer", 3), "SCARD"))
        results.append(assert_eq(send(sock, "SISMEMBER", "s1", "m2"), ("integer", 1), "SISMEMBER m2"))
        results.append(assert_eq(send(sock, "SISMEMBER", "s1", "mx"), ("integer", 0), "SISMEMBER mx"))
        results.append(assert_eq(send(sock, "SREM", "s1", "m2"), ("integer", 1), "SREM m2"))

        # ===== Sorted Set =====
        results.append(assert_eq(send(sock, "ZADD", "z1", "1", "one", "2", "two", "3", "three"), ("integer", 3), "ZADD 3"))
        results.append(assert_eq(send(sock, "ZCARD", "z1"), ("integer", 3), "ZCARD"))
        results.append(assert_eq(send(sock, "ZSCORE", "z1", "two"), ("bulk", "2"), "ZSCORE"))
        results.append(assert_eq(send(sock, "ZRANGE", "z1", 0, -1),
                               ("array", [("bulk","one"),("bulk","two"),("bulk","three")]), "ZRANGE by rank"))

        # ===== Expiration =====
        results.append(assert_eq(send(sock, "SET", "exp", "v", "EX", "100"), ("simple","OK"), "SET EX 100"))
        results.append(assert_eq(send(sock, "TTL", "exp"), ("integer", 100), "TTL (approx)"))
        results.append(assert_eq(send(sock, "PERSIST", "exp"), ("integer", 1), "PERSIST"))

        # ===== Type =====
        results.append(assert_eq(send(sock, "TYPE", "k1"), ("bulk", "string"), "TYPE string"))
        results.append(assert_eq(send(sock, "TYPE", "h1"), ("bulk", "hash"), "TYPE hash"))
        results.append(assert_eq(send(sock, "TYPE", "l1"), ("bulk", "list"), "TYPE list"))
        results.append(assert_eq(send(sock, "TYPE", "s1"), ("bulk", "set"), "TYPE set"))
        results.append(assert_eq(send(sock, "TYPE", "z1"), ("bulk", "zset"), "TYPE zset"))

        # ===== Unknown command =====
        unknown = send(sock, "BOGUS")
        results.append(unknown[0] == "error" and "unknown command" in unknown[1].lower())

        # ===== Persistence subcommands =====
        results.append(assert_eq(send(sock, "CLIENT", "GETNAME"), ("bulk", None), "CLIENT GETNAME"))
        results.append(assert_eq(send(sock, "ACL", "WHOAMI"), ("bulk", "default"), "ACL WHOAMI"))
        results.append(assert_eq(send(sock, "COMMAND", "COUNT")[0], "integer", "COMMAND COUNT"))

        # ===== DBSIZE =====
        # At least the keys we created
        dbsize = send(sock, "DBSIZE")
        results.append(dbsize[0] == "integer" and dbsize[1] >= 6)

        sock.close()
        passed = sum(1 for r in results if r)
        total = len(results)
        print("\n=== %d/%d passed ===" % (passed, total))
        return 0 if passed == total else 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
