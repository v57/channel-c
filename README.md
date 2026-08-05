<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. C implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Minimal dependencies, protocol-compatible with the TypeScript reference.

# Installation

Dependencies (installed via Homebrew):

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
