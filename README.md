<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. C implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Minimal dependencies, protocol-compatible with the TypeScript reference.

# Installation

Dependencies: [libwebsockets](https://libwebsockets.org) (WebSocket client and server) with OpenSSL, and
[yyjson](https://github.com/ibireme/yyjson) (JSON). `cmocka`, `lcov` and `bun` are for the tests only.
Installed via Homebrew:

```bash
brew install cmake libwebsockets yyjson cmocka lcov pkgconf bun
```

# Build
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j8
```

# Usage

```c
// Server + process transport
#include "channel-c/channel_c.h"

static int hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return 0;
}

int main(void) {
  channel_c_channel_t *ch = channel_c_channel_new();
  channel_c_channel_post(ch, "hello", hello, NULL, 0);

  // Process transport:
  channel_c_process_t *proc = channel_c_process_new(ch, NULL, NULL);
  channel_c_sender_t *sender = channel_c_process_sender(proc);

  char *body = NULL;
  char *error = NULL;
  channel_c_sender_send_string(sender, "hello", NULL, &body, &error);

  channel_c_free(body);
  channel_c_free(error);
  channel_c_process_free(proc);
  channel_c_channel_free(ch);
  return 0;
}
```

# Transports
- `client` (WebSocket client)
- `server` (WebSocket server)
- `process` (in-process transport)
- `quic` client and server (experimental, optional, see below)

# QUIC transport (experimental)

The same protocol over QUIC (spec: `channel/docs/quic.md` in the TypeScript reference): one bidirectional stream
per connection, each message prefixed with its length. It is a separate library, `channel_c_quic`, built only
with `-DCHANNEL_QUIC=ON` (off by default), so a WebSocket-only build doesn't include it. It uses OpenSSL 3.5+'s
native QUIC (client and server), which the WebSocket transport already links: no extra library.

```bash
cmake -S . -B build -DCHANNEL_QUIC=ON
cmake --build build -j8
# link channel_c_quic (it brings channel_c with it)
```

```c
#include "channel-c/channel_c_quic.h"

// Server: same handlers, state factory (from the client's hello headers), events and hooks as the WebSocket server
channel_c_quic_server_options_t quic = { cert_pem, key_pem };
channel_c_quic_server_t *server = channel_c_quic_server_listen(channel, "0.0.0.0", 4433, state_factory, &quic, &error);
// ...channel_c_quic_server_poll(server) in your loop, like channel_c_server_poll...
channel_c_quic_server_free(server);

// Client: options.headers become the hello headers (the WebSocket upgrade headers' equivalent)
channel_c_quic_tls_options_t tls = { .ca_pem = cert_pem };
channel_c_quic_client_t *client = channel_c_quic_client_connect(channel, "quic://127.0.0.1:4433", &options, &tls, &error);
channel_c_sender_send(channel_c_quic_client_sender(client), "hello", NULL, NULL, &body, &error);
channel_c_quic_client_free(client);
```

Certificates: the server always needs a PEM certificate and key (QUIC has no plaintext mode). The client checks the
server certificate against the system roots, or:
- `ca_pem`: PEM certificate(s) to trust instead, for self-signed servers. The URL host must be in the certificate's
  subject alternative names (an IP address is checked as one);
- `fingerprint`: the certificate's SHA-256 (hex, colons optional, `openssl x509 -in cert.pem -noout -fingerprint
  -sha256`). Pins it and replaces every other check;
- `server_name`: the name sent (SNI) and checked, instead of the URL host.

Every client connection dials from its own UDP socket, never a shared one: Network.framework servers (Swift's
HubServer) take one connection per client address and port. Like this port's WebSocket client, a QUIC client
doesn't reconnect: when the connection drops, waiting requests fail with `disconnected`. The client sends a
keepalive every 15 s and both sides use a 30 s idle timeout.

Which languages support QUIC: see `channel/docs/quic.md`.

# Run tests
```bash
./scripts/test
```

# Run benchmark
```bash
./scripts/benchmark
```

# Run coverage (LCOV)
```bash
./scripts/coverage
# output: coverage/lcov.info
```

# Notes
- JSON body/context are passed as JSON text (for example `"42"`, `"\"text\""`, `"{\"a\":1}"`).
- `ObjectMap` optimization from JS is intentionally not replicated in C.
- Integration tests run against the TypeScript fixture server (`../channel/tests/server.ts`) on `127.0.0.1:2049`, plus C client -> C server compatibility tests and process transport tests.
- `scripts/test` builds with `-DCHANNEL_QUIC=ON` and runs the QUIC tests too: frames, C client -> C server, certificate
  trust, the wire rules with raw frames, the C client against the fixture's QUIC server (UDP, same port;
  `CHANNEL_TEST_PORT` picks another) and the TypeScript QUIC client (`tests/quic/client.ts`, run with bun) against
  the C server.
