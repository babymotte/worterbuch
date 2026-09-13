#!/usr/bin/env python3
"""
quic_nc.py - a minimal, netcat-like command line client for manually testing
worterbuch's QUIC endpoint (see ../src/server/quic.rs).

There is no netcat equivalent for QUIC in the Arch repos (or really anywhere)
because, unlike TCP, QUIC always requires a TLS 1.3 handshake plus ALPN
protocol negotiation before any bytes can flow - a generic byte-pipe tool
would still need to know what certificate policy and ALPN name to use. This
script hardcodes worterbuch's choices (ALPN "worterbuch") and otherwise
behaves like `nc host port`: stdin goes to the server, whatever the server
sends comes back on stdout.

worterbuch speaks the same single, ordered, newline delimited JSON protocol
over QUIC as it does over TCP and Unix sockets, but with one twist: it is the
*server* that opens the one bidirectional stream used for that protocol, not
the client. That's because the server speaks first (it sends a `Welcome`
message unprompted), and QUIC requires whoever calls open_bi() to write to it
before the peer's accept_bi() can succeed. This script waits for the server
to open that stream and then pipes stdin/stdout through it, same as netcat
would through a TCP socket.

Requires the aioquic library (not installed by default):

    sudo pacman -S python-aioquic

Usage:

    python3 quic_nc.py [--insecure | --ca-cert FILE] HOST PORT

Example, against a locally running server with a self-signed test cert:

    $ python3 quic_nc.py --insecure 127.0.0.1 8082
    {"welcome":{"clientId":"...","info":{...}}}
    {"get":{"key":"some/key","transactionId":1}}
    {"state":{"transactionId":1,"event":{"keyValue":{"key":"some/key","value":42}}}}

Press Ctrl-D to stop sending (the connection stays open for further server
output until it is closed or you press Ctrl-C).
"""

import argparse
import asyncio
import ssl
import sys

from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived

# Must match ALPN_PROTOCOL in worterbuch/src/server/quic.rs.
ALPN_PROTOCOL = "worterbuch"


class NetcatProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_id: int | None = None
        self.stream_ready = asyncio.Event()

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            # The server opens the stream; the first time we see data on it
            # is how we learn which stream_id to write our own lines to.
            if self.stream_id is None:
                self.stream_id = event.stream_id
                self.stream_ready.set()
            sys.stdout.buffer.write(event.data)
            sys.stdout.buffer.flush()

    async def send_line(self, data: bytes) -> None:
        await self.stream_ready.wait()
        self._quic.send_stream_data(self.stream_id, data, end_stream=False)
        self.transmit()


async def pump_stdin(protocol: NetcatProtocol) -> None:
    loop = asyncio.get_event_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.buffer.readline)
        if not line:
            break
        await protocol.send_line(line)


def build_configuration(args: argparse.Namespace) -> QuicConfiguration:
    configuration = QuicConfiguration(is_client=True, alpn_protocols=[ALPN_PROTOCOL])
    if args.insecure:
        configuration.verify_mode = ssl.CERT_NONE
    elif args.ca_cert:
        configuration.load_verify_locations(args.ca_cert)
    return configuration


async def run(args: argparse.Namespace) -> None:
    configuration = build_configuration(args)
    async with connect(
        args.host,
        args.port,
        configuration=configuration,
        create_protocol=NetcatProtocol,
    ) as protocol:
        await pump_stdin(protocol)
        # stdin closed; keep printing whatever the server still sends until
        # it closes the connection (or the user hits Ctrl-C).
        await protocol.wait_closed()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="netcat-like command line client for worterbuch's QUIC endpoint",
    )
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    verify = parser.add_mutually_exclusive_group()
    verify.add_argument(
        "--insecure",
        action="store_true",
        help="skip TLS certificate verification (e.g. for self-signed test certs)",
    )
    verify.add_argument(
        "--ca-cert",
        help="path to a PEM file to verify the server certificate against",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
