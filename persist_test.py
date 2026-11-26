"""
Persistence test: verifies that all write commands survive a server restart.
This was the original critical bug (AOF missed list/set/hash/zset mutations).
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


def read_reply(s):
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
        return ("$", d.decode())
    if t == b"*":
        n = int(read_line(s))
        return ("*", [read_reply(s) for _ in range(n)])
    raise AssertionError(t)


def cmd(s, *args):
    s.sendall(encode(*args))
    return read_reply(s)


def start_server():
    proc = subprocess.Popen(
        ["java", "-Dfile.encoding=UTF-8", "-jar", JAR],
        stdout=open("server.log", "w"), stderr=subprocess.STDOUT,
    )
    for _ in range(60):
        try:
            with socket.create_connection((HOST, PORT), timeout=1):
                return proc
        except OSError:
            time.sleep(0.3)
    proc.terminate()
    raise RuntimeError("server failed to start")


def main():
    for f in ("dump.rdb", "appendonly.aof"):
        if os.path.exists(f): os.remove(f)

    print("=== Phase 1: write data ===")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        cmd(s, "FLUSHALL")
        cmd(s, "SET",     "string:k1", "Hello")
        cmd(s, "RPUSH",   "list:k1",   "a", "b", "c")
        cmd(s, "SADD",    "set:k1",    "m1", "m2", "m3")
        cmd(s, "HSET",    "hash:k1",   "f1", "v1", "f2", "v2")
        cmd(s, "ZADD",    "zset:k1",   "1", "one", "2", "two", "3", "three")
        cmd(s, "INCR",    "counter")
        cmd(s, "SAVE")  # forces RDB save
        s.close()
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()

    print("=== Phase 2: restart and verify ===")
    proc = start_server()
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
        results = []

        def check(label, actual, expected):
            ok = actual == expected
            print(("OK   " if ok else "FAIL ") + label + " got=" + repr(actual))
            results.append(ok)

        check("GET string:k1",   cmd(s, "GET",     "string:k1"),  ("$", "Hello"))
        check("LRANGE list:k1",  cmd(s, "LRANGE",  "list:k1", 0, -1),
              ("*", [("$","a"),("$","b"),("$","c")]))
        check("SCARD set:k1",    cmd(s, "SCARD",   "set:k1"),     (":", 3))
        check("HGETALL hash:k1", cmd(s, "HGETALL", "hash:k1"),
              ("*", [("$","f1"),("$","v1"),("$","f2"),("$","v2")]))
        check("ZCARD zset:k1",   cmd(s, "ZCARD",   "zset:k1"),    (":", 3))
        check("GET counter",     cmd(s, "GET",     "counter"),    ("$", "1"))
        s.close()
        print("\n=== %d/6 passed ===" % sum(results))
        return 0 if all(results) else 1
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


if __name__ == "__main__":
    sys.exit(main())
