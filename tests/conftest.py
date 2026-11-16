import ipaddress
import socket
import sys

import pytest


class _NetworkBlockedSupabase:
    def table(self, name):
        raise AssertionError(
            f"Test attempted to use an unmocked Supabase table: {name}"
        )


@pytest.fixture(autouse=True)
def block_unmocked_supabase_and_network(monkeypatch):
    from supabase_helpers import supabase_client, supabase_connection

    original_clients = {
        id(supabase_connection.supabase),
        id(supabase_connection.supabase_admin),
        id(supabase_client.supabase),
    }
    blocked_client = _NetworkBlockedSupabase()
    for module in tuple(sys.modules.values()):
        namespace = getattr(module, "__dict__", None)
        if not namespace:
            continue
        for name, value in tuple(namespace.items()):
            if id(value) in original_clients:
                monkeypatch.setattr(module, name, blocked_client, raising=False)

    network_attempts = []
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_create_connection = socket.create_connection
    original_getaddrinfo = socket.getaddrinfo

    def is_loopback_address(address):
        host = address[0] if isinstance(address, tuple) else address
        if host == "localhost":
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False

    def blocked_connect(self, address):
        if is_loopback_address(address):
            return original_connect(self, address)
        network_attempts.append("socket.connect")
        raise AssertionError("Network access is disabled in tests: socket.connect")

    def blocked_connect_ex(self, address):
        if is_loopback_address(address):
            return original_connect_ex(self, address)
        network_attempts.append("socket.connect_ex")
        raise AssertionError("Network access is disabled in tests: socket.connect_ex")

    def blocked_create_connection(address, *args, **kwargs):
        if is_loopback_address(address):
            return original_create_connection(address, *args, **kwargs)
        network_attempts.append("socket.create_connection")
        raise AssertionError("Network access is disabled in tests: socket.create_connection")

    def blocked_getaddrinfo(host, *args, **kwargs):
        if is_loopback_address(host):
            return original_getaddrinfo(host, *args, **kwargs)
        network_attempts.append("socket.getaddrinfo")
        raise AssertionError("Network access is disabled in tests: socket.getaddrinfo")

    monkeypatch.setattr(socket.socket, "connect", blocked_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked_connect_ex)
    monkeypatch.setattr(socket, "create_connection", blocked_create_connection)
    monkeypatch.setattr(socket, "getaddrinfo", blocked_getaddrinfo)

    yield

    assert not network_attempts, (
        "Tests attempted network access: " + ", ".join(network_attempts)
    )
