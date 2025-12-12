#!/usr/bin/env python3
import base64
import errno
import logging
import select
import socket
import socketserver
import threading
from typing import List, Tuple

# ------------- CONFIG -------------

PROXIES_FILE = "proxies.txt"   # host:port:user:pass per line
LOCAL_HOST = "127.0.0.1"
START_PORT = 10000             # first local port to use

# optional: max number of proxies to use (None = all in file)
MAX_PROXIES = None

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("webshare-proxy")

# ------------- UTIL -------------

def load_proxies(path: str) -> List[Tuple[str, int, str, str]]:
    """
    Read proxies.txt with lines: host:port:user:password
    Returns list of (host, port, user, password)
    """
    proxies: List[Tuple[str, int, str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) != 4:
                log.warning(f"[SKIP] Bad proxy line: {line!r}")
                continue
            host, port_str, user, pwd = parts
            try:
                port = int(port_str)
            except ValueError:
                log.warning(f"[SKIP] Invalid port in line: {line!r}")
                continue
            proxies.append((host, port, user, pwd))
    return proxies

# ------------- PROXY HANDLER FACTORY -------------

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


def make_proxy_handler(up_host: str, up_port: int, up_user: str, up_pwd: str, local_port: int):
    """
    Create a handler class bound to one upstream Webshare proxy.
    Each local port gets its own handler class instance with its config.
    """

    auth_bytes = f"{up_user}:{up_pwd}".encode("utf-8")
    auth_header = b"Proxy-Authorization: Basic " + base64.b64encode(auth_bytes) + b"\r\n"

    class ProxyHandler(socketserver.BaseRequestHandler):
        def handle(self):
            client_ip, client_port = self.client_address
            log.info(
                f"[CONN] {client_ip}:{client_port} -> {LOCAL_HOST}:{local_port} "
                f"via {up_host}:{up_port} ({up_user})"
            )

            try:
                upstream = socket.create_connection((up_host, up_port), timeout=10)
            except Exception as e:
                log.error(f"[FAIL] Connect upstream {up_host}:{up_port} ({up_user}): {e}")
                return

            try:
                # ---- First request: inject Proxy-Authorization if missing ----
                try:
                    first_chunk = self.request.recv(65535)
                except Exception as e:
                    log.error(f"[READ] Error from client {client_ip}:{client_port}: {e}")
                    upstream.close()
                    return

                if not first_chunk:
                    upstream.close()
                    return

                to_send = first_chunk

                # Only touch if it looks like HTTP and doesn't already have Proxy-Authorization
                try:
                    head, sep, rest = first_chunk.partition(b"\r\n\r\n")
                    if head and sep and b"proxy-authorization:" not in head.lower():
                        # Insert header after first line
                        lines = head.split(b"\r\n")
                        if lines:
                            new_head = b"\r\n".join([lines[0]] + [auth_header.rstrip(b"\r\n")] + lines[1:])
                            to_send = new_head + sep + rest
                            log.debug(f"[AUTH] Injected Proxy-Authorization for {client_ip}:{client_port}")
                except Exception as e:
                    log.debug(f"[AUTH] Failed to parse/adjust headers (will pass as is): {e}")

                try:
                    upstream.sendall(to_send)
                except Exception as e:
                    log.error(f"[UPSTREAM SEND] Error: {e}")
                    upstream.close()
                    return

                # ---- Relay both directions ----
                sockets = [self.request, upstream]
                while True:
                    rlist, _, _ = select.select(sockets, [], [], 60)
                    if not rlist:
                        # Timeout, treat as idle
                        break

                    if self.request in rlist:
                        try:
                            data = self.request.recv(65535)
                        except Exception as e:
                            log.debug(f"[CLIENT READ] Error: {e}")
                            break
                        if not data:
                            break
                        try:
                            upstream.sendall(data)
                        except Exception as e:
                            log.debug(f"[UPSTREAM WRITE] Error: {e}")
                            break

                    if upstream in rlist:
                        try:
                            data = upstream.recv(65535)
                        except Exception as e:
                            log.debug(f"[UPSTREAM READ] Error: {e}")
                            break
                        if not data:
                            break
                        try:
                            self.request.sendall(data)
                        except Exception as e:
                            log.debug(f"[CLIENT WRITE] Error: {e}")
                            break

            finally:
                try:
                    upstream.close()
                except Exception:
                    pass
                try:
                    self.request.close()
                except Exception:
                    pass
                log.info(f"[CLOSE] {client_ip}:{client_port} on {LOCAL_HOST}:{local_port}")

    return ProxyHandler

# ------------- MAIN -------------

def main():
    proxies = load_proxies(PROXIES_FILE)
    if not proxies:
        log.error("[FATAL] No valid proxies loaded from proxies.txt")
        return

    if MAX_PROXIES is not None:
        proxies = proxies[:MAX_PROXIES]

    log.info(f"[INFO] Loaded {len(proxies)} proxies")
    servers: List[ThreadedTCPServer] = []

    for i, (host, port, user, pwd) in enumerate(proxies):
        local_port = START_PORT + i
        handler_cls = make_proxy_handler(host, port, user, pwd, local_port)

        try:
            server = ThreadedTCPServer((LOCAL_HOST, local_port), handler_cls)
        except OSError as e:
            # common Windows errors: permission or already in use
            if getattr(e, "errno", None) in (errno.EACCES, 10013, 10048):
                log.warning(
                    f"[SKIP] Cannot bind {LOCAL_HOST}:{local_port} ({e}). Skipping this port."
                )
                continue
            log.error(f"[ERROR] Failed to bind {LOCAL_HOST}:{local_port}: {e!r}")
            continue

        servers.append(server)
        log.info(
            f"[MAP] {LOCAL_HOST}:{local_port} -> {host}:{port} ({user}:{pwd})"
        )

        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

    if not servers:
        log.error("[FATAL] No servers started. All ports failed to bind.")
        return

    log.info(
        "[READY] Local proxies running.\n"
        f"        First: {LOCAL_HOST}:{START_PORT}\n"
        f"        Last:  {LOCAL_HOST}:{START_PORT + len(servers) - 1}\n"
        "        Press Ctrl+C to stop."
    )

    try:
        while True:
            threading.Event().wait(3600)
    except KeyboardInterrupt:
        log.info("[SHUTDOWN] Stopping all servers...")
        for s in servers:
            s.shutdown()
            s.server_close()
        log.info("[SHUTDOWN] Done.")

if __name__ == "__main__":
    main()
