#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "quic_internal.h"

typedef enum {
  QUIC_CLIENT_CONNECTING = 0,
  QUIC_CLIENT_OPEN = 1,
  QUIC_CLIENT_CLOSED = 2,
} quic_client_phase_t;

struct channel_c_quic_client {
  int running;
  quic_client_phase_t phase;
  unsigned long progress_counter;
  char *last_error;

  SSL_CTX *ctx;
  SSL *ssl;
  SSL *stream;
  unsigned char fingerprint[32];
  int pinned;

  char *host;
  int port;

  channel_c_quic_buffer_t tx;
  channel_c_quic_frame_reader_t rx;
  int64_t last_keepalive_ms;
  /* Sent first once the stream is open */
  char *hello;

  channel_c_connection_t *connection;
};

/* Sends CONNECTION_CLOSE right away, without waiting for the peer, and frees the QUIC objects. */
static void quic_client_close_now(struct channel_c_quic_client *client) {
  if (!client) return;
  if (client->stream) {
    (void)channel_c_quic_flush(client->stream, &client->tx);
    (void)SSL_stream_conclude(client->stream, 0);
  }
  if (client->ssl) {
    int64_t deadline = channel_c_quic_now_ms() + 100;
    while (SSL_shutdown_ex(client->ssl, SSL_SHUTDOWN_FLAG_RAPID, NULL, 0) == 0 && channel_c_quic_now_ms() < deadline) {
      (void)SSL_handle_events(client->ssl);
      channel_c_internal_sleep_short();
    }
  }
  ERR_clear_error();
  SSL_free(client->stream);
  client->stream = NULL;
  SSL_free(client->ssl);
  client->ssl = NULL;
  SSL_CTX_free(client->ctx);
  client->ctx = NULL;
  client->phase = QUIC_CLIENT_CLOSED;
}

static void quic_client_fail(struct channel_c_quic_client *client, char *error_text) {
  if (!client) {
    free(error_text);
    return;
  }
  if (!client->last_error) {
    client->last_error = error_text ? error_text : channel_c_strdup("connection error");
  } else {
    free(error_text);
  }
  quic_client_close_now(client);
  client->progress_counter += 1;
  channel_c_connection_on_close(client->connection);
}

static int quic_client_send_text(void *ctx, const char *text) {
  struct channel_c_quic_client *client = (struct channel_c_quic_client *)ctx;
  if (!client || !text || !client->running || client->phase == QUIC_CLIENT_CLOSED) return -1;
  if (channel_c_quic_buffer_append_frame(&client->tx, text, strlen(text)) != 0) return -1;
  return 0;
}

static void quic_client_transport_close(void *ctx) {
  struct channel_c_quic_client *client = (struct channel_c_quic_client *)ctx;
  if (!client) return;
  client->running = 0;
  quic_client_close_now(client);
}

static int quic_client_check_pin(struct channel_c_quic_client *client) {
  X509 *certificate = SSL_get0_peer_certificate(client->ssl);
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_length = 0;
  if (!certificate || !X509_digest(certificate, EVP_sha256(), digest, &digest_length)) return -1;
  if (digest_length != sizeof(client->fingerprint)) return -1;
  return memcmp(digest, client->fingerprint, sizeof(client->fingerprint)) == 0 ? 0 : -1;
}

static void quic_client_open_stream(struct channel_c_quic_client *client) {
  if (client->pinned && quic_client_check_pin(client) != 0) {
    quic_client_fail(client, channel_c_strdup("certificate fingerprint mismatch"));
    return;
  }
  client->stream = SSL_new_stream(client->ssl, SSL_STREAM_FLAG_NO_BLOCK);
  if (!client->stream) {
    quic_client_fail(client, channel_c_quic_ssl_error("failed to open a quic stream"));
    return;
  }
  SSL_set_mode(client->stream, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  /* The hello goes in front of anything queued while connecting */
  {
    channel_c_quic_buffer_t queued = client->tx;
    memset(&client->tx, 0, sizeof(client->tx));
    if (channel_c_quic_buffer_append_frame(&client->tx, client->hello, strlen(client->hello)) != 0 ||
        channel_c_quic_buffer_append(&client->tx, queued.data + queued.start, channel_c_quic_buffer_length(&queued)) != 0) {
      channel_c_quic_buffer_free(&queued);
      quic_client_fail(client, channel_c_strdup("internal error"));
      return;
    }
    channel_c_quic_buffer_free(&queued);
  }
  client->phase = QUIC_CLIENT_OPEN;
  client->last_keepalive_ms = channel_c_quic_now_ms();
  client->progress_counter += 1;
  channel_c_connection_mark_connected(client->connection, 1);
}

static void quic_client_handshake(struct channel_c_quic_client *client) {
  int result = SSL_connect(client->ssl);
  if (result == 1) {
    quic_client_open_stream(client);
    return;
  }
  switch (SSL_get_error(client->ssl, result)) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
      ERR_clear_error();
      return;
    default:
      {
        long verify = SSL_get_verify_result(client->ssl);
        if (verify != X509_V_OK) {
          const char *reason = X509_verify_cert_error_string(verify);
          size_t length = strlen(reason) + 32;
          char *text = (char *)malloc(length);
          if (text) snprintf(text, length, "certificate verify failed: %s", reason);
          ERR_clear_error();
          quic_client_fail(client, text);
        } else {
          quic_client_fail(client, channel_c_quic_ssl_error("quic handshake failed"));
        }
      }
  }
}

static int quic_client_poll_local(void *ctx) {
  struct channel_c_quic_client *client = (struct channel_c_quic_client *)ctx;
  unsigned long start;

  if (!client || !client->running || !client->ssl || client->phase == QUIC_CLIENT_CLOSED) return 0;
  start = client->progress_counter;
  (void)SSL_handle_events(client->ssl);

  if (client->phase == QUIC_CLIENT_CONNECTING) {
    quic_client_handshake(client);
    if (client->phase != QUIC_CLIENT_OPEN) return client->progress_counter != start;
  }

  for (int iteration = 0; iteration < 32; iteration += 1) {
    unsigned long before_iteration = client->progress_counter;
    char *text = NULL;
    int frame;
    int read_result;

    if (client->running && channel_c_quic_buffer_length(&client->tx) == 0 &&
        channel_c_quic_now_ms() - client->last_keepalive_ms >= CHANNEL_C_QUIC_KEEPALIVE_MS) {
      unsigned char keepalive = 0;
      (void)channel_c_quic_buffer_append(&client->tx, &keepalive, 1);
      client->last_keepalive_ms = channel_c_quic_now_ms();
    }
    if (channel_c_quic_buffer_length(&client->tx) > 0) {
      size_t before = channel_c_quic_buffer_length(&client->tx);
      if (channel_c_quic_flush(client->stream, &client->tx) != 0) {
        quic_client_fail(client, channel_c_strdup("disconnected"));
        break;
      }
      if (channel_c_quic_buffer_length(&client->tx) != before) {
        client->progress_counter += 1;
        client->last_keepalive_ms = channel_c_quic_now_ms();
      }
    }

    read_result = channel_c_quic_read(client->stream, &client->rx);
    if (read_result < 0) {
      quic_client_fail(client, channel_c_strdup("disconnected"));
      break;
    }
    if (read_result > 0) client->progress_counter += 1;

    frame = 0;
    while (client->phase == QUIC_CLIENT_OPEN &&
           (frame = channel_c_quic_frame_reader_next(&client->rx, &text, NULL)) == 1) {
      client->progress_counter += 1;
      channel_c_connection_receive_text(client->connection, text);
      free(text);
      text = NULL;
      if (channel_c_connection_is_closed(client->connection)) break;
    }
    if (client->phase != QUIC_CLIENT_OPEN || !client->running) break;
    if (frame < 0) {
      quic_client_fail(client, channel_c_strdup("message too big"));
      break;
    }
    if (channel_c_connection_is_closed(client->connection)) {
      /* Invalid JSON from the server */
      quic_client_fail(client, channel_c_strdup("invalid message"));
      break;
    }
    /* Handlers may have queued replies: send them in this poll */
    if (client->progress_counter == before_iteration) break;
  }

  return client->progress_counter != start ? 1 : 0;
}

static int parse_fingerprint(const char *text, unsigned char out[32]) {
  size_t count = 0;
  int high = -1;
  for (; *text; text += 1) {
    int value;
    if (*text == ':') continue;
    if (!isxdigit((unsigned char)*text)) return -1;
    value = isdigit((unsigned char)*text) ? *text - '0' : (tolower((unsigned char)*text) - 'a' + 10);
    if (high < 0) {
      high = value;
    } else {
      if (count == 32) return -1;
      out[count++] = (unsigned char)((high << 4) | value);
      high = -1;
    }
  }
  return count == 32 && high < 0 ? 0 : -1;
}

/* quic://host:port[/...]; IPv6 hosts in brackets. */
static int parse_quic_url(const char *url, char **out_host, int *out_port) {
  const char *cursor;
  const char *host_start;
  const char *host_end;
  char *end = NULL;
  long port;

  if (strncmp(url, "quic://", 7) != 0) return -1;
  cursor = url + 7;
  if (*cursor == '[') {
    host_start = cursor + 1;
    host_end = strchr(host_start, ']');
    if (!host_end) return -1;
    cursor = host_end + 1;
  } else {
    host_start = cursor;
    while (*cursor && *cursor != ':' && *cursor != '/') cursor += 1;
    host_end = cursor;
  }
  if (host_end == host_start || *cursor != ':') return -1;
  port = strtol(cursor + 1, &end, 10);
  if (end == cursor + 1 || port <= 0 || port > 65535 || (*end && *end != '/')) return -1;
  *out_host = (char *)malloc((size_t)(host_end - host_start) + 1);
  if (!*out_host) return -1;
  memcpy(*out_host, host_start, (size_t)(host_end - host_start));
  (*out_host)[host_end - host_start] = '\0';
  *out_port = (int)port;
  return 0;
}

static int load_ca(SSL_CTX *ctx, const char *pem) {
  BIO *bio = BIO_new_mem_buf(pem, -1);
  X509_STORE *store = SSL_CTX_get_cert_store(ctx);
  X509 *certificate;
  int count = 0;
  if (!bio || !store) {
    BIO_free(bio);
    return -1;
  }
  while ((certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL))) {
    if (X509_STORE_add_cert(store, certificate)) count += 1;
    X509_free(certificate);
  }
  BIO_free(bio);
  ERR_clear_error();
  return count > 0 ? 0 : -1;
}

static int is_ip_address(const char *host) {
  unsigned char buffer[16];
  return inet_pton(AF_INET, host, buffer) == 1 || inet_pton(AF_INET6, host, buffer) == 1;
}

/* A connected UDP socket of its own (one per connection, reconnects included), as a datagram BIO. */
static int quic_client_socket(struct channel_c_quic_client *client, char **out_error) {
  BIO_ADDRINFO *results = NULL;
  const BIO_ADDRINFO *chosen = NULL;
  const BIO_ADDRINFO *info;
  char port_text[16];
  BIO *bio;
  int sock;

  snprintf(port_text, sizeof(port_text), "%d", client->port);
  if (!BIO_lookup_ex(client->host, port_text, BIO_LOOKUP_CLIENT, AF_UNSPEC, SOCK_DGRAM, IPPROTO_UDP, &results)) {
    *out_error = channel_c_strdup("failed to resolve the quic host");
    return -1;
  }
  /* IPv4 first: listeners on 0.0.0.0 don't answer on ::1 */
  for (info = results; info; info = BIO_ADDRINFO_next(info)) {
    if (BIO_ADDRINFO_family(info) == AF_INET) {
      chosen = info;
      break;
    }
  }
  if (!chosen) chosen = results;

  sock = BIO_socket(BIO_ADDRINFO_family(chosen), SOCK_DGRAM, IPPROTO_UDP, 0);
  if (sock < 0 || !BIO_connect(sock, BIO_ADDRINFO_address(chosen), 0) || !BIO_socket_nbio(sock, 1)) {
    if (sock >= 0) BIO_closesocket(sock);
    BIO_ADDRINFO_free(results);
    *out_error = channel_c_strdup("failed to open a udp socket");
    return -1;
  }
  bio = BIO_new(BIO_s_datagram());
  if (!bio) {
    BIO_closesocket(sock);
    BIO_ADDRINFO_free(results);
    *out_error = channel_c_strdup("internal error");
    return -1;
  }
  BIO_set_fd(bio, sock, BIO_CLOSE);
  SSL_set_bio(client->ssl, bio, bio);
  if (!SSL_set1_initial_peer_addr(client->ssl, BIO_ADDRINFO_address(chosen))) {
    BIO_ADDRINFO_free(results);
    *out_error = channel_c_quic_ssl_error("failed to set the quic peer address");
    return -1;
  }
  BIO_ADDRINFO_free(results);
  return 0;
}

static int quic_client_configure(
  struct channel_c_quic_client *client,
  const channel_c_quic_tls_options_t *tls,
  char **out_error
) {
  const char *server_name = tls && tls->server_name && tls->server_name[0] ? tls->server_name : NULL;
  int by_ip = !server_name && is_ip_address(client->host);
  const char *check_name = server_name ? server_name : client->host;

  client->ctx = SSL_CTX_new(OSSL_QUIC_client_method());
  if (!client->ctx) {
    *out_error = channel_c_quic_ssl_error("failed to create a quic context");
    return -1;
  }
  if (client->pinned) {
    /* Pinning replaces the other checks: the fingerprint is compared after the handshake */
    SSL_CTX_set_verify(client->ctx, SSL_VERIFY_NONE, NULL);
  } else {
    SSL_CTX_set_verify(client->ctx, SSL_VERIFY_PEER, NULL);
    if (tls && tls->ca_pem && tls->ca_pem[0]) {
      if (load_ca(client->ctx, tls->ca_pem) != 0) {
        *out_error = channel_c_strdup("ca has no PEM certificate");
        return -1;
      }
    } else if (!SSL_CTX_set_default_verify_paths(client->ctx)) {
      *out_error = channel_c_quic_ssl_error("failed to load the system certificates");
      return -1;
    }
  }

  client->ssl = SSL_new(client->ctx);
  if (!client->ssl) {
    *out_error = channel_c_quic_ssl_error("failed to create a quic connection");
    return -1;
  }
  if (!by_ip && !SSL_set_tlsext_host_name(client->ssl, check_name)) {
    *out_error = channel_c_quic_ssl_error("invalid server name");
    return -1;
  }
  if (!client->pinned) {
    int set = by_ip ? X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(client->ssl), client->host)
                    : SSL_set1_host(client->ssl, check_name);
    if (!set) {
      *out_error = channel_c_quic_ssl_error("invalid server name");
      return -1;
    }
  }
  if (SSL_set_alpn_protos(client->ssl, (const unsigned char *)CHANNEL_C_QUIC_ALPN_WIRE, CHANNEL_C_QUIC_ALPN_WIRE_LENGTH) != 0 ||
      !SSL_set_feature_request_uint(client->ssl, SSL_VALUE_QUIC_IDLE_TIMEOUT, CHANNEL_C_QUIC_IDLE_TIMEOUT_MS) ||
      !SSL_set_default_stream_mode(client->ssl, SSL_DEFAULT_STREAM_MODE_NONE) ||
      !SSL_set_incoming_stream_policy(client->ssl, SSL_INCOMING_STREAM_POLICY_REJECT, 0) ||
      !SSL_set_blocking_mode(client->ssl, 0)) {
    *out_error = channel_c_quic_ssl_error("failed to configure the quic connection");
    return -1;
  }
  return quic_client_socket(client, out_error);
}

static void quic_client_destroy(struct channel_c_quic_client *client) {
  if (!client) return;
  quic_client_close_now(client);
  channel_c_quic_buffer_free(&client->tx);
  channel_c_quic_frame_reader_free(&client->rx);
  free(client->hello);
  free(client->host);
  free(client->last_error);
  if (client->connection) channel_c_connection_release(client->connection);
  free(client);
}

channel_c_quic_client_t *channel_c_quic_client_connect(
  channel_c_channel_t *channel,
  const char *url,
  const channel_c_client_options_t *options,
  const channel_c_quic_tls_options_t *tls,
  char **out_error
) {
  struct channel_c_quic_client *client;
  channel_c_transport_ops_t transport_ops;
  char *error = NULL;
  int64_t deadline;
  size_t index;

  if (out_error) *out_error = NULL;
  if (!channel || !url) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }
  client = (struct channel_c_quic_client *)calloc(1, sizeof(struct channel_c_quic_client));
  if (!client) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }
  client->running = 1;
  channel_c_quic_frame_reader_init(&client->rx, CHANNEL_C_MAX_MESSAGE_SIZE);

  if (parse_quic_url(url, &client->host, &client->port) != 0) {
    error = channel_c_strdup("invalid quic url");
    goto fail;
  }
  if (tls && tls->fingerprint) {
    if (parse_fingerprint(tls->fingerprint, client->fingerprint) != 0) {
      error = channel_c_strdup("fingerprint isn't a SHA-256 in hex");
      goto fail;
    }
    client->pinned = 1;
  }
  if (options && options->header_count > 0) {
    if (!options->headers) {
      error = channel_c_strdup("invalid header");
      goto fail;
    }
    for (index = 0; index < options->header_count; index += 1) {
      if (!options->headers[index].name || !options->headers[index].name[0] || !options->headers[index].value) {
        error = channel_c_strdup("invalid header");
        goto fail;
      }
    }
  }
  client->hello = channel_c_quic_hello_json(options ? options->headers : NULL, options ? options->header_count : 0);
  if (!client->hello) {
    error = channel_c_strdup("internal error");
    goto fail;
  }
  if (quic_client_configure(client, tls, &error) != 0) goto fail;

  transport_ops.send_text = quic_client_send_text;
  transport_ops.close = quic_client_transport_close;
  client->connection = channel_c_connection_create(
    channel,
    options ? options->state : NULL,
    options ? options->state_destroy : NULL,
    NULL,
    client,
    CHANNEL_C_TRANSPORT_REMOTE,
    transport_ops
  );
  if (!client->connection) {
    error = channel_c_strdup("internal error");
    goto fail;
  }

  channel_c_internal_register_pump(client, quic_client_poll_local);
  deadline = channel_c_quic_now_ms() + 10000;
  while (client->phase == QUIC_CLIENT_CONNECTING) {
    if (!channel_c_internal_pump_all()) channel_c_internal_sleep_short();
    if (client->phase == QUIC_CLIENT_CONNECTING && channel_c_quic_now_ms() > deadline) {
      quic_client_fail(client, channel_c_strdup("quic handshake timed out"));
    }
  }
  if (client->phase != QUIC_CLIENT_OPEN) {
    error = client->last_error ? channel_c_strdup(client->last_error) : channel_c_strdup("failed to connect");
    channel_c_internal_unregister_pump(client);
    goto fail;
  }
  return client;

fail:
  if (client->connection) {
    client->running = 0;
    channel_c_connection_stop(client->connection);
  }
  quic_client_destroy(client);
  if (out_error) *out_error = error ? error : channel_c_strdup("failed to connect");
  else free(error);
  return NULL;
}

channel_c_sender_t *channel_c_quic_client_sender(channel_c_quic_client_t *client) {
  return client ? channel_c_connection_sender(client->connection) : NULL;
}

int channel_c_quic_client_poll(channel_c_quic_client_t *client) {
  return quic_client_poll_local(client);
}

int channel_c_quic_client_is_connected(channel_c_quic_client_t *client) {
  if (!client) return 0;
  return client->phase == QUIC_CLIENT_OPEN && channel_c_connection_is_connected(client->connection);
}

int channel_c_quic_client_remote_is_subscribed(channel_c_quic_client_t *client, const char *topic) {
  return client ? channel_c_connection_is_remote_subscribed(client->connection, topic) : 0;
}

void channel_c_quic_client_stop(channel_c_quic_client_t *client) {
  if (!client) return;
  channel_c_internal_unregister_pump(client);
  if (client->running) {
    client->running = 0;
    quic_client_close_now(client);
  }
  channel_c_connection_stop(client->connection);
}

void channel_c_quic_client_free(channel_c_quic_client_t *client) {
  if (!client) return;
  channel_c_quic_client_stop(client);
  quic_client_destroy(client);
}
