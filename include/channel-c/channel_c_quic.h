#ifndef CHANNEL_C_CHANNEL_C_QUIC_H
#define CHANNEL_C_CHANNEL_C_QUIC_H

/* QUIC transport (experimental, see channel/docs/quic.md): one bidirectional stream per connection
 * carrying the same JSON messages as a WebSocket, each one prefixed with its length as a QUIC
 * variable-length integer. Built with -DCHANNEL_QUIC=ON into the separate `channel_c_quic` library,
 * on OpenSSL 3.5+'s native QUIC. */

#include <stddef.h>
#include <stdint.h>

#include "channel-c/channel_c.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CHANNEL_C_QUIC_ALPN "channel/1"

typedef struct channel_c_quic_client channel_c_quic_client_t;
typedef struct channel_c_quic_server channel_c_quic_server_t;

/* How the client checks the server certificate. Zero-initialize for the system roots. */
typedef struct {
  /* PEM certificate(s) to trust instead of the system roots, for self-signed servers. */
  const char *ca_pem;
  /* SHA-256 fingerprint of the server certificate (hex, colons optional). Pins it and replaces
   * every other check. */
  const char *fingerprint;
  /* Name sent (SNI) and checked; defaults to the URL host (an IP address is checked as one). */
  const char *server_name;
} channel_c_quic_tls_options_t;

/* Connects to `quic://host:port` from its own UDP socket and waits up to 10 s for the handshake.
 * `options` (state, hello headers) and `tls` may be NULL. NULL on failure, with *out_error set. */
channel_c_quic_client_t *channel_c_quic_client_connect(
  channel_c_channel_t *channel,
  const char *url,
  const channel_c_client_options_t *options,
  const channel_c_quic_tls_options_t *tls,
  char **out_error
);
channel_c_sender_t *channel_c_quic_client_sender(channel_c_quic_client_t *client);
int channel_c_quic_client_poll(channel_c_quic_client_t *client);
int channel_c_quic_client_is_connected(channel_c_quic_client_t *client);
int channel_c_quic_client_remote_is_subscribed(channel_c_quic_client_t *client, const char *topic);
void channel_c_quic_client_stop(channel_c_quic_client_t *client);
void channel_c_quic_client_free(channel_c_quic_client_t *client);

typedef struct {
  /* PEM certificate chain and private key. */
  const char *cert_pem;
  const char *key_pem;
} channel_c_quic_server_options_t;

/* Serves `channel` over QUIC on UDP host:port (host NULL: 127.0.0.1; port 0: any free port). The
 * state factory gets the client's hello headers, lowercased, as `create_with_headers`. */
channel_c_quic_server_t *channel_c_quic_server_listen(
  channel_c_channel_t *channel,
  const char *host,
  int port,
  channel_c_state_factory_t state_factory,
  const channel_c_quic_server_options_t *options,
  char **out_error
);
int channel_c_quic_server_poll(channel_c_quic_server_t *server);
int channel_c_quic_server_port(channel_c_quic_server_t *server);
int channel_c_quic_server_connection_count(channel_c_quic_server_t *server);
void channel_c_quic_server_stop(channel_c_quic_server_t *server);
void channel_c_quic_server_free(channel_c_quic_server_t *server);

/* RFC 9000 variable-length integer of `value` (< 2^62) into `out`; returns its size (1, 2, 4 or 8). */
size_t channel_c_quic_varint_encode(uint64_t value, unsigned char out[8]);

#ifdef __cplusplus
}
#endif

#endif
