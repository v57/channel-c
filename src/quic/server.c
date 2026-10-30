#include <stdlib.h>
#include <string.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include "quic_internal.h"

typedef struct quic_server_client quic_server_client_t;

/* One QUIC connection: its first bidirectional stream carries the channel connection, which is
 * created once the hello frame arrives. */
struct quic_server_client {
  struct channel_c_quic_server *server;
  SSL *ssl;
  SSL *stream;
  channel_c_connection_t *connection;
  /* Set when the connection must go; the poll then shuts it down and frees it */
  int closing;
  int64_t closing_deadline_ms;

  channel_c_quic_buffer_t tx;
  channel_c_quic_frame_reader_t rx;

  quic_server_client_t *next;
};

struct channel_c_quic_server {
  int running;
  unsigned long progress_counter;

  SSL_CTX *ctx;
  SSL *listener;
  channel_c_channel_t *channel;
  int port;
  channel_c_state_factory_t state_factory;

  quic_server_client_t *clients;
};

static int quic_server_client_send_text(void *ctx, const char *text) {
  quic_server_client_t *client = (quic_server_client_t *)ctx;
  if (!client || !text || client->closing) return -1;
  return channel_c_quic_buffer_append_frame(&client->tx, text, strlen(text));
}

static void quic_server_client_close(void *ctx) {
  quic_server_client_t *client = (quic_server_client_t *)ctx;
  if (!client || client->closing) return;
  client->closing = 1;
  client->closing_deadline_ms = channel_c_quic_now_ms() + 2000;
  /* What the handlers already sent still goes out before CONNECTION_CLOSE */
  if (client->stream) {
    (void)channel_c_quic_flush(client->stream, &client->tx);
    (void)SSL_stream_conclude(client->stream, 0);
  }
  if (client->server) client->server->progress_counter += 1;
}

/* Ends the channel connection: pending requests fail, onDisconnect runs. */
static void quic_server_client_detach(quic_server_client_t *client) {
  channel_c_connection_t *connection = client->connection;
  struct channel_c_quic_server *server = client->server;
  if (!connection) return;
  client->connection = NULL;
  channel_c_connection_on_close(connection);
  if (server && server->state_factory.on_disconnect) {
    server->state_factory.on_disconnect(
      channel_c_connection_state(connection),
      channel_c_connection_sender(connection),
      server->state_factory.user_data
    );
  }
  channel_c_connection_release(connection);
}

static void quic_server_client_free(quic_server_client_t *client) {
  quic_server_client_detach(client);
  ERR_clear_error();
  SSL_free(client->stream);
  SSL_free(client->ssl);
  channel_c_quic_buffer_free(&client->tx);
  channel_c_quic_frame_reader_free(&client->rx);
  free(client);
}

/* First frame: {"hello": headers}, used like the WebSocket upgrade request headers. */
static int quic_server_client_hello(quic_server_client_t *client, const char *text, size_t length) {
  struct channel_c_quic_server *server = client->server;
  channel_c_header_t *items = NULL;
  size_t count = 0;
  channel_c_transport_ops_t transport_ops;
  void *state = NULL;

  if (channel_c_quic_hello_parse(text, length, &items, &count) != 0) return -1;
  if (server->state_factory.create_with_headers) {
    channel_c_headers_t headers;
    headers.items = items;
    headers.count = count;
    state = server->state_factory.create_with_headers(&headers, server->state_factory.user_data);
  } else if (server->state_factory.create) {
    state = server->state_factory.create(server->state_factory.user_data);
  }
  channel_c_quic_headers_free(items, count);

  transport_ops.send_text = quic_server_client_send_text;
  transport_ops.close = quic_server_client_close;
  client->connection = channel_c_connection_create(
    server->channel,
    state,
    server->state_factory.destroy,
    server->state_factory.user_data,
    client,
    CHANNEL_C_TRANSPORT_REMOTE,
    transport_ops
  );
  if (!client->connection) {
    if (server->state_factory.destroy && state) server->state_factory.destroy(state, server->state_factory.user_data);
    return -1;
  }
  channel_c_connection_mark_connected(client->connection, 1);
  if (server->state_factory.on_connect) {
    server->state_factory.on_connect(
      channel_c_connection_state(client->connection),
      channel_c_connection_sender(client->connection),
      server->state_factory.user_data
    );
  }
  return 0;
}

static void quic_server_reset_stream(SSL *stream) {
  SSL_STREAM_RESET_ARGS args;
  memset(&args, 0, sizeof(args));
  (void)SSL_stream_reset(stream, &args, sizeof(args));
  SSL_free(stream);
}

/* One stream per connection: the client's first bidirectional one. Later and unidirectional
 * streams are refused. */
static void quic_server_client_accept_streams(quic_server_client_t *client) {
  SSL *stream;
  while ((stream = SSL_accept_stream(client->ssl, SSL_ACCEPT_STREAM_NO_BLOCK))) {
    client->server->progress_counter += 1;
    if (!client->stream && SSL_get_stream_type(stream) == SSL_STREAM_TYPE_BIDI) {
      client->stream = stream;
      SSL_set_mode(stream, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
      (void)SSL_set_incoming_stream_policy(client->ssl, SSL_INCOMING_STREAM_POLICY_REJECT, 0);
    } else {
      quic_server_reset_stream(stream);
    }
  }
  ERR_clear_error();
}

/* Reads, dispatches and writes for one connection. Closes it on any protocol error. */
static void quic_server_client_service(quic_server_client_t *client) {
  struct channel_c_quic_server *server = client->server;
  int iteration;

  quic_server_client_accept_streams(client);
  if (!client->stream) {
    SSL_CONN_CLOSE_INFO info;
    if (SSL_get_conn_close_info(client->ssl, &info, sizeof(info))) quic_server_client_close(client);
    ERR_clear_error();
    return;
  }

  for (iteration = 0; iteration < 32 && !client->closing; iteration += 1) {
    unsigned long before = server->progress_counter;
    int read_result = channel_c_quic_read(client->stream, &client->rx);
    char *text = NULL;
    size_t length = 0;
    int frame = 0;

    if (read_result > 0) server->progress_counter += 1;
    while (!client->closing && (frame = channel_c_quic_frame_reader_next(&client->rx, &text, &length)) == 1) {
      server->progress_counter += 1;
      if (!client->connection) {
        if (quic_server_client_hello(client, text, length) != 0) {
          free(text);
          quic_server_client_close(client);
          return;
        }
      } else {
        channel_c_connection_receive_text(client->connection, text);
        /* Invalid JSON closes the channel connection: the QUIC one goes too */
        if (channel_c_connection_is_closed(client->connection)) quic_server_client_close(client);
      }
      free(text);
      text = NULL;
    }
    if (client->closing) return;
    if (frame < 0 || read_result < 0) {
      quic_server_client_close(client);
      return;
    }
    if (channel_c_quic_buffer_length(&client->tx) > 0) {
      size_t pending = channel_c_quic_buffer_length(&client->tx);
      if (channel_c_quic_flush(client->stream, &client->tx) != 0) {
        quic_server_client_close(client);
        return;
      }
      if (channel_c_quic_buffer_length(&client->tx) != pending) server->progress_counter += 1;
    }
    if (server->progress_counter == before) break;
  }
}

/* Removes connections that finished closing; returns whether any went. */
static void quic_server_reap(struct channel_c_quic_server *server) {
  quic_server_client_t **slot = &server->clients;
  while (*slot) {
    quic_server_client_t *client = *slot;
    if (client->closing) {
      int done;
      quic_server_client_detach(client);
      done = SSL_shutdown_ex(client->ssl, SSL_SHUTDOWN_FLAG_NO_BLOCK, NULL, 0) != 0 ||
             channel_c_quic_now_ms() > client->closing_deadline_ms;
      ERR_clear_error();
      if (done) {
        *slot = client->next;
        quic_server_client_free(client);
        server->progress_counter += 1;
        continue;
      }
    }
    slot = &client->next;
  }
}

static int quic_server_poll_local(void *ctx) {
  struct channel_c_quic_server *server = (struct channel_c_quic_server *)ctx;
  quic_server_client_t *client;
  unsigned long start;
  SSL *ssl;

  if (!server || !server->running || !server->listener) return 0;
  start = server->progress_counter;
  (void)SSL_handle_events(server->listener);

  while ((ssl = SSL_accept_connection(server->listener, SSL_ACCEPT_CONNECTION_NO_BLOCK))) {
    quic_server_client_t *accepted = (quic_server_client_t *)calloc(1, sizeof(quic_server_client_t));
    server->progress_counter += 1;
    if (!accepted) {
      SSL_free(ssl);
      continue;
    }
    accepted->server = server;
    accepted->ssl = ssl;
    channel_c_quic_frame_reader_init(&accepted->rx, CHANNEL_C_MAX_MESSAGE_SIZE);
    if (!SSL_set_blocking_mode(ssl, 0) ||
        !SSL_set_default_stream_mode(ssl, SSL_DEFAULT_STREAM_MODE_NONE) ||
        !SSL_set_incoming_stream_policy(ssl, SSL_INCOMING_STREAM_POLICY_ACCEPT, 0)) {
      accepted->closing = 1;
      accepted->closing_deadline_ms = channel_c_quic_now_ms();
    }
    accepted->next = server->clients;
    server->clients = accepted;
  }
  ERR_clear_error();

  for (client = server->clients; client; client = client->next) {
    if (!client->closing) quic_server_client_service(client);
  }
  quic_server_reap(server);
  return server->progress_counter != start ? 1 : 0;
}

static int quic_server_alpn(
  SSL *ssl,
  const unsigned char **out,
  unsigned char *out_length,
  const unsigned char *in,
  unsigned int in_length,
  void *arg
) {
  unsigned char *selected = NULL;
  (void)ssl;
  (void)arg;
  if (SSL_select_next_proto(&selected, out_length, (const unsigned char *)CHANNEL_C_QUIC_ALPN_WIRE,
                            CHANNEL_C_QUIC_ALPN_WIRE_LENGTH, in, in_length) != OPENSSL_NPN_NEGOTIATED) {
    return SSL_TLSEXT_ERR_ALERT_FATAL;
  }
  *out = selected;
  return SSL_TLSEXT_ERR_OK;
}

static int quic_server_load_identity(SSL_CTX *ctx, const char *cert_pem, const char *key_pem) {
  BIO *bio = BIO_new_mem_buf(cert_pem, -1);
  X509 *certificate;
  EVP_PKEY *key;
  int ok = 0;

  if (!bio) return -1;
  certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL);
  if (certificate && SSL_CTX_use_certificate(ctx, certificate)) {
    X509 *chain;
    ok = 1;
    while ((chain = PEM_read_bio_X509(bio, NULL, NULL, NULL))) {
      if (!SSL_CTX_add0_chain_cert(ctx, chain)) {
        X509_free(chain);
        ok = 0;
        break;
      }
    }
  }
  X509_free(certificate);
  BIO_free(bio);
  ERR_clear_error();
  if (!ok) return -1;

  bio = BIO_new_mem_buf(key_pem, -1);
  if (!bio) return -1;
  key = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
  BIO_free(bio);
  ok = key && SSL_CTX_use_PrivateKey(ctx, key) && SSL_CTX_check_private_key(ctx);
  EVP_PKEY_free(key);
  ERR_clear_error();
  return ok ? 0 : -1;
}

static int quic_server_bind(struct channel_c_quic_server *server, const char *host, int port, char **out_error) {
  BIO_ADDRINFO *results = NULL;
  BIO_ADDR *local = NULL;
  char port_text[16];
  BIO *bio;
  int sock;

  snprintf(port_text, sizeof(port_text), "%d", port);
  if (!BIO_lookup_ex(host, port_text, BIO_LOOKUP_SERVER, AF_UNSPEC, SOCK_DGRAM, IPPROTO_UDP, &results)) {
    *out_error = channel_c_strdup("failed to resolve the listen address");
    return -1;
  }
  sock = BIO_socket(BIO_ADDRINFO_family(results), SOCK_DGRAM, IPPROTO_UDP, 0);
  if (sock < 0 || !BIO_bind(sock, BIO_ADDRINFO_address(results), BIO_SOCK_REUSEADDR) || !BIO_socket_nbio(sock, 1)) {
    if (sock >= 0) BIO_closesocket(sock);
    BIO_ADDRINFO_free(results);
    ERR_clear_error();
    *out_error = channel_c_strdup("failed to bind the quic server address");
    return -1;
  }
  BIO_ADDRINFO_free(results);

  local = BIO_ADDR_new();
  if (local) {
    union BIO_sock_info_u info;
    info.addr = local;
    if (BIO_sock_info(sock, BIO_SOCK_INFO_ADDRESS, &info)) server->port = ntohs(BIO_ADDR_rawport(local));
    BIO_ADDR_free(local);
  }
  if (server->port <= 0) server->port = port;

  bio = BIO_new(BIO_s_datagram());
  if (!bio) {
    BIO_closesocket(sock);
    *out_error = channel_c_strdup("internal error");
    return -1;
  }
  BIO_set_fd(bio, sock, BIO_CLOSE);
  SSL_set_bio(server->listener, bio, bio);
  return 0;
}

channel_c_quic_server_t *channel_c_quic_server_listen(
  channel_c_channel_t *channel,
  const char *host,
  int port,
  channel_c_state_factory_t state_factory,
  const channel_c_quic_server_options_t *options,
  char **out_error
) {
  struct channel_c_quic_server *server;
  char *error = NULL;

  if (out_error) *out_error = NULL;
  if (!channel || port < 0 || port > 65535 || !options || !options->cert_pem || !options->key_pem) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }
  server = (struct channel_c_quic_server *)calloc(1, sizeof(struct channel_c_quic_server));
  if (!server) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }
  server->channel = channel;
  server->state_factory = state_factory;

  server->ctx = SSL_CTX_new(OSSL_QUIC_server_method());
  if (!server->ctx) {
    error = channel_c_quic_ssl_error("failed to create a quic context");
    goto fail;
  }
  if (quic_server_load_identity(server->ctx, options->cert_pem, options->key_pem) != 0) {
    error = channel_c_strdup("invalid certificate or key");
    goto fail;
  }
  SSL_CTX_set_verify(server->ctx, SSL_VERIFY_NONE, NULL);
  SSL_CTX_set_alpn_select_cb(server->ctx, quic_server_alpn, NULL);
  /* OpenSSL's default max_idle_timeout is the protocol's 30 s. (Setting it per connection from the
   * pending-connection callback deadlocks in 3.5: the callback runs under the engine lock.) */

  server->listener = SSL_new_listener(server->ctx, 0);
  if (!server->listener) {
    error = channel_c_quic_ssl_error("failed to create a quic listener");
    goto fail;
  }
  if (quic_server_bind(server, host && host[0] ? host : "127.0.0.1", port, &error) != 0) goto fail;
  if (!SSL_set_blocking_mode(server->listener, 0) || !SSL_listen(server->listener)) {
    error = channel_c_quic_ssl_error("failed to listen");
    goto fail;
  }

  server->running = 1;
  channel_c_internal_register_pump(server, quic_server_poll_local);
  return server;

fail:
  SSL_free(server->listener);
  SSL_CTX_free(server->ctx);
  free(server);
  ERR_clear_error();
  if (out_error) *out_error = error ? error : channel_c_strdup("failed to listen");
  else free(error);
  return NULL;
}

int channel_c_quic_server_poll(channel_c_quic_server_t *server) {
  return quic_server_poll_local(server);
}

int channel_c_quic_server_port(channel_c_quic_server_t *server) {
  return server ? server->port : 0;
}

int channel_c_quic_server_connection_count(channel_c_quic_server_t *server) {
  quic_server_client_t *client;
  int count = 0;
  if (!server) return 0;
  for (client = server->clients; client; client = client->next) {
    if (client->connection && !client->closing) count += 1;
  }
  return count;
}

void channel_c_quic_server_stop(channel_c_quic_server_t *server) {
  quic_server_client_t *client;
  int64_t deadline;

  if (!server || !server->running) return;
  server->running = 0;
  channel_c_internal_unregister_pump(server);

  /* Every connection gets CONNECTION_CLOSE right away; then the port is freed */
  for (client = server->clients; client; client = client->next) {
    quic_server_client_detach(client);
    (void)SSL_shutdown_ex(client->ssl, SSL_SHUTDOWN_FLAG_RAPID | SSL_SHUTDOWN_FLAG_NO_BLOCK, NULL, 0);
  }
  deadline = channel_c_quic_now_ms() + 100;
  while (server->clients && channel_c_quic_now_ms() < deadline) {
    int pending = 0;
    (void)SSL_handle_events(server->listener);
    for (client = server->clients; client; client = client->next) {
      if (SSL_shutdown_ex(client->ssl, SSL_SHUTDOWN_FLAG_RAPID | SSL_SHUTDOWN_FLAG_NO_BLOCK, NULL, 0) == 0) pending = 1;
    }
    if (!pending) break;
    channel_c_internal_sleep_short();
  }
  while (server->clients) {
    client = server->clients;
    server->clients = client->next;
    quic_server_client_free(client);
  }
  ERR_clear_error();
  SSL_free(server->listener);
  server->listener = NULL;
  SSL_CTX_free(server->ctx);
  server->ctx = NULL;
}

void channel_c_quic_server_free(channel_c_quic_server_t *server) {
  if (!server) return;
  channel_c_quic_server_stop(server);
  free(server);
}
