"""READ-ONLY. Determines whether the ~0.09 MB/s throughput is a TCP_NODELAY /
delayed-ACK stall rather than genuine bandwidth, by measuring raw socket options
and comparing throughput with NODELAY forced on and off.

The profile showed ~33 ms per TLS read, which is the classic signature of Nagle
interacting with delayed ACKs.
"""

import asyncio
import os
import socket
import sys
import time

import pymongo
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri, redact_uri  # noqa: E402
from main import build_event_org_conditions  # noqa: E402

NOR_CELLS = {
    "$nor": [
        {"Event Type": {"$regex": "Cells", "$options": "i"}},
        {"eventType": {"$regex": "Cells", "$options": "i"}},
        {"eventTypeName": {"$regex": "Cells", "$options": "i"}},
    ]
}
QUERY = {"$and": [
    {"$or": build_event_org_conditions("active-teams", "Active Church")}, NOR_CELLS,
]}


def raw_throughput(nodelay, runs=2):
    """Fetch the query with pymongo sync, forcing TCP_NODELAY on or off."""
    client = pymongo.MongoClient(resolve_mongo_uri())
    events = client["active-teams-db"]["Events"]

    # Reach the socket pymongo opened for this topology.
    def sockets():
        for srv in client._topology._servers.values():
            for s in getattr(srv, "conns", {}).values():
                yield s.conn

    # warm
    list(events.find(QUERY, {"_id": 1}).limit(5))

    times = []
    for _ in range(runs):
        for s in sockets():
            try:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if nodelay else 0)
            except Exception:
                pass
        t0 = time.perf_counter()
        docs = list(events.find(QUERY))
        times.append(time.perf_counter() - t0)

    nodelay_state = None
    for s in sockets():
        try:
            nodelay_state = s.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        except Exception:
            pass
        break

    client.close()
    return min(times), len(docs), nodelay_state


def rtt():
    """Round-trip time to the Atlas host, measured at the socket level."""
    import ssl
    from urllib.parse import urlparse
    host = urlparse(resolve_mongo_uri()).hostname
    port = 27017
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        s = socket.create_connection((host, port), timeout=15)
        t0 = time.perf_counter()
        s = ctx.wrap_socket(s, server_hostname=host)
        tls_ms = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        for _ in range(5):
            s.send(b"\x00" * 100)
            s.recv(1)
        rtt_ms = (time.perf_counter() - t0) / 5 * 1000
        s.close()
        return tls_ms, rtt_ms
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


if __name__ == "__main__":
    print(f"URI: {redact_uri(resolve_mongo_uri())}\n")
    print("=== TLS handshake + RTT to Atlas ===")
    tls, rtt = rtt()
    if tls is not None:
        print(f"  TLS handshake : {tls:.0f} ms")
        print(f"  per round trip : {rtt:.1f} ms")
    else:
        print(f"  failed: {rtt}")

    print("\n=== throughput with TCP_NODELAY forced OFF vs ON ===")
    for nodelay in (False, True):
        secs, n, state = raw_throughput(nodelay)
        print(f"  NODELAY={'on ' if nodelay else 'off'}  {n:>3} docs  "
              f"{secs:>6.2f}s  ({4.35 / secs:.2f} MB/s)  "
              f"socket NODELAY was {state}")
