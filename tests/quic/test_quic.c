/* QUIC transport: C client against C server, certificate trust, the wire rules with raw frames,
 * and the TypeScript reference's QUIC client against the C server (tests/quic/client.ts). */
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <cmocka.h>
#include <openssl/err.h>
#include <yyjson.h>

#include "quic_internal.h"

#define FINGERPRINT "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"

static char *g_cert;
static char *g_key;
static int g_connected;
static int g_disconnected;

typedef struct {
  char *name;
} test_state_t;

static char *read_file(const char *path) {
  FILE *file = fopen(path, "rb");
  long size;
  char *data;
  if (!file) return NULL;
  fseek(file, 0, SEEK_END);
  size = ftell(file);
  fseek(file, 0, SEEK_SET);
  data = (char *)malloc((size_t)size + 1);
  if (data && fread(data, 1, (size_t)size, file) != (size_t)size) {
    free(data);
    data = NULL;
  }
  if (data) data[size] = '\0';
  fclose(file);
  return data;
}

static char *json_string(const char *text) {
  size_t length = text ? strlen(text) : 0;
  char *out = (char *)malloc(length + 3);
  if (!out) return NULL;
  snprintf(out, length + 3, "\"%s\"", text ? text : "");
  return out;
}

/* Server channel */

static channel_c_channel_t *g_server_channel;

static void *state_create(const channel_c_headers_t *headers, void *user_data) {
  test_state_t *state = (test_state_t *)calloc(1, sizeof(test_state_t));
  const char *name = channel_c_headers_get(headers, "x-name");
  (void)user_data;
  if (state && name) state->name = channel_c_strdup(name);
  return state;
}

static void state_destroy(void *state, void *user_data) {
  test_state_t *test_state = (test_state_t *)state;
  (void)user_data;
  if (!test_state) return;
  free(test_state->name);
  free(test_state);
}

static void on_connect(void *state, channel_c_sender_t *sender, void *user_data) {
  (void)state;
  (void)sender;
  (void)user_data;
  g_connected += 1;
}

static void on_disconnect(void *state, channel_c_sender_t *sender, void *user_data) {
  (void)state;
  (void)sender;
  (void)user_data;
  g_disconnected += 1;
}

static int route_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return 0;
}

static int route_echo(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_error;
  *out_body_json = channel_c_strdup(request->body_json ? request->body_json : "null");
  return 0;
}

static int route_name(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  test_state_t *state = (test_state_t *)request->state;
  (void)out_error;
  *out_body_json = state && state->name ? json_string(state->name) : NULL;
  return 0;
}

static int route_disconnect(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_body_json;
  (void)out_error;
  channel_c_sender_stop(request->sender);
  return 0;
}

static int route_tick(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_body_json;
  return channel_c_channel_event_send(g_server_channel, "ticks", "\"a\"", "\"tick\"", out_error);
}

static void route_count(const channel_c_stream_request_t *request) {
  int index;
  for (index = 0; index < 3; index += 1) {
    char value[16];
    snprintf(value, sizeof(value), "%d", index);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
  }
}

static channel_c_quic_server_t *listen_quic(int port) {
  channel_c_quic_server_options_t options = { g_cert, g_key };
  channel_c_state_factory_t factory;
  channel_c_quic_server_t *server;
  char *error = NULL;

  memset(&factory, 0, sizeof(factory));
  factory.create_with_headers = state_create;
  factory.destroy = state_destroy;
  factory.on_connect = on_connect;
  factory.on_disconnect = on_disconnect;
  server = channel_c_quic_server_listen(g_server_channel, "127.0.0.1", port, factory, &options, &error);
  if (error) fprintf(stderr, "listen: %s\n", error);
  assert_null(error);
  assert_non_null(server);
  assert_true(channel_c_quic_server_port(server) > 0);
  return server;
}

static int setup(void **state) {
  (void)state;
  g_cert = read_file(CHANNEL_TS_DIR "/tests/quic/cert.pem");
  g_key = read_file(CHANNEL_TS_DIR "/tests/quic/key.pem");
  if (!g_cert || !g_key) {
    fprintf(stderr, "the TypeScript reference's test certificate isn't next to this checkout\n");
    return -1;
  }
  g_server_channel = channel_c_channel_new();
  channel_c_channel_post(g_server_channel, "hello", route_hello, NULL, 0);
  channel_c_channel_post(g_server_channel, "echo", route_echo, NULL, 0);
  channel_c_channel_post(g_server_channel, "name", route_name, NULL, 0);
  channel_c_channel_post(g_server_channel, "disconnect", route_disconnect, NULL, 0);
  channel_c_channel_post(g_server_channel, "tick", route_tick, NULL, 0);
  channel_c_channel_stream(g_server_channel, "count", route_count, NULL);
  channel_c_channel_subscription(g_server_channel, "ticks", NULL, NULL);
  return 0;
}

static int teardown(void **state) {
  (void)state;
  channel_c_channel_free(g_server_channel);
  free(g_cert);
  free(g_key);
  return 0;
}

static void pump_for(int milliseconds) {
  int64_t deadline = channel_c_quic_now_ms() + milliseconds;
  while (channel_c_quic_now_ms() < deadline) {
    if (!channel_c_internal_pump_all()) usleep(200);
  }
}

static int pump_until(int (*ready)(void *), void *context, int milliseconds) {
  int64_t deadline = channel_c_quic_now_ms() + milliseconds;
  while (!ready(context)) {
    if (channel_c_quic_now_ms() > deadline) return 0;
    if (!channel_c_internal_pump_all()) usleep(200);
  }
  return 1;
}

static char *url_for(channel_c_quic_server_t *server) {
  char *url = (char *)malloc(64);
  snprintf(url, 64, "quic://127.0.0.1:%d", channel_c_quic_server_port(server));
  return url;
}

static channel_c_quic_client_t *connect_quic(
  channel_c_channel_t *channel,
  const char *url,
  const channel_c_quic_tls_options_t *tls,
  char **out_error
) {
  channel_c_header_t headers[] = { { "X-Name", "c" } };
  channel_c_client_options_t options;
  memset(&options, 0, sizeof(options));
  options.headers = headers;
  options.header_count = 1;
  return channel_c_quic_client_connect(channel, url, &options, tls, out_error);
}

static void expect(channel_c_sender_t *sender, const char *path, const char *body, const char *want) {
  char *got = NULL;
  char *error = NULL;
  int result = channel_c_sender_send(sender, path, body, NULL, &got, &error);
  if (error) fprintf(stderr, "%s: %s\n", path, error);
  assert_int_equal(result, 0);
  assert_null(error);
  if (want) {
    assert_non_null(got);
    assert_string_equal(got, want);
  } else {
    assert_null(got);
  }
  channel_c_free(got);
}

static void on_tick(const char *body_json, void *user_data) {
  int *count = (int *)user_data;
  if (body_json && strcmp(body_json, "\"tick\"") == 0) *count += 1;
}

typedef struct {
  int *value;
  int want;
} int_wait_t;

static int int_reached(void *context) {
  int_wait_t *wait = (int_wait_t *)context;
  return *wait->value == wait->want;
}

static int client_gone(void *context) {
  void **pair = (void **)context;
  return !channel_c_quic_client_is_connected((channel_c_quic_client_t *)pair[0]) &&
         channel_c_quic_server_connection_count((channel_c_quic_server_t *)pair[1]) == 0;
}

static void test_client_and_server(void **state) {
  channel_c_quic_server_t *server;
  channel_c_channel_t *client_channel = channel_c_channel_new();
  channel_c_quic_tls_options_t tls = { NULL, NULL, NULL };
  channel_c_quic_client_t *client;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_subscription_handle_t *handle = NULL;
  char *url;
  char *error = NULL;
  char *body = NULL;
  char *big;
  size_t index;
  int done = 0;
  int ticks = 0;
  int_wait_t wait;
  void *pair[2];
  (void)state;

  g_connected = 0;
  g_disconnected = 0;
  server = listen_quic(0);
  url = url_for(server);
  tls.ca_pem = g_cert;
  client = connect_quic(client_channel, url, &tls, &error);
  if (error) fprintf(stderr, "connect: %s\n", error);
  assert_non_null(client);
  assert_true(channel_c_quic_client_is_connected(client));
  sender = channel_c_quic_client_sender(client);

  /* post */
  expect(sender, "hello", NULL, "\"world\"");
  big = (char *)malloc(20000 * 2 + 64);
  strcpy(big, "{\"list\":[1,\"two\",null],\"text\":\"");
  {
    size_t offset = strlen(big);
    for (index = 0; index < 20000; index += 1) {
      big[offset++] = (char)0xc3;
      big[offset++] = (char)0xa9;
    }
    strcpy(big + offset, "\"}");
  }
  expect(sender, "echo", big, big);
  free(big);

  /* hello headers become the state, names lowercased */
  expect(sender, "name", NULL, "\"c\"");
  assert_int_equal(g_connected, 1);
  assert_int_equal(channel_c_quic_server_connection_count(server), 1);

  /* stream */
  values = channel_c_sender_values(sender, "count", NULL, NULL, &error);
  assert_non_null(values);
  for (index = 0; !done; index += 1) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    if (!done) {
      char want[8];
      snprintf(want, sizeof(want), "%zu", index);
      assert_string_equal(body, want);
      channel_c_free(body);
      body = NULL;
    }
  }
  assert_int_equal(index, 4);
  channel_c_values_close(values);

  /* events: only the subscribed topic, nothing after cancel */
  assert_int_equal(channel_c_sender_subscribe(sender, "ticks", "\"a\"", NULL, on_tick, &ticks, &handle, &error), 0);
  assert_null(error);
  assert_int_equal(channel_c_channel_event_send(g_server_channel, "ticks", "\"a\"", "\"tick\"", &error), 0);
  assert_int_equal(channel_c_channel_event_send(g_server_channel, "ticks", "\"b\"", "\"tick\"", &error), 0);
  assert_int_equal(channel_c_channel_event_send(g_server_channel, "ticks", "\"a\"", "\"tick\"", &error), 0);
  wait.value = &ticks;
  wait.want = 2;
  assert_true(pump_until(int_reached, &wait, 5000));
  channel_c_subscription_cancel(handle);
  pump_for(50);
  assert_int_equal(channel_c_channel_event_send(g_server_channel, "ticks", "\"a\"", "\"tick\"", &error), 0);
  pump_for(100);
  assert_int_equal(ticks, 2);

  /* many requests, one after another */
  for (index = 0; index < 2000; index += 1) {
    char number[16];
    snprintf(number, sizeof(number), "%zu", index);
    expect(sender, "echo", number, number);
  }

  /* The server drops the connection: what waits fails, as over WebSocket */
  (void)channel_c_sender_send(sender, "disconnect", NULL, NULL, &body, &error);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  pair[0] = client;
  pair[1] = server;
  assert_true(pump_until(client_gone, pair, 5000));
  assert_int_not_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;
  wait.value = &g_disconnected;
  wait.want = 1;
  assert_true(pump_until(int_reached, &wait, 2000));

  channel_c_quic_client_free(client);

  /* A server that stops closes its connections and frees the port */
  client = connect_quic(client_channel, url, &tls, &error);
  assert_non_null(client);
  expect(channel_c_quic_client_sender(client), "hello", NULL, "\"world\"");
  {
    int port = channel_c_quic_server_port(server);
    channel_c_quic_server_free(server);
    {
      int64_t deadline = channel_c_quic_now_ms() + 3000;
      while (channel_c_quic_client_is_connected(client) && channel_c_quic_now_ms() < deadline) {
        (void)channel_c_quic_client_poll(client);
        usleep(500);
      }
    }
    assert_false(channel_c_quic_client_is_connected(client));
    server = listen_quic(port);
    assert_int_equal(channel_c_quic_server_port(server), port);
  }
  channel_c_quic_client_free(client);
  assert_int_equal(g_connected, g_disconnected);

  channel_c_quic_server_free(server);
  channel_c_channel_free(client_channel);
  free(url);
}

static void expect_refused(const char *url, const channel_c_quic_tls_options_t *tls, const char *label) {
  channel_c_channel_t *channel = channel_c_channel_new();
  char *error = NULL;
  channel_c_quic_client_t *client = connect_quic(channel, url, tls, &error);
  if (client) fprintf(stderr, "%s: connected\n", label);
  assert_null(client);
  assert_non_null(error);
  channel_c_free(error);
  channel_c_channel_free(channel);
}

static void test_certificate_trust(void **state) {
  channel_c_quic_server_t *server = listen_quic(0);
  channel_c_channel_t *channel = channel_c_channel_new();
  char *url = url_for(server);
  char *error = NULL;
  channel_c_quic_client_t *client;
  char pin[65];
  size_t index;
  size_t out = 0;
  (void)state;

  /* Pinned, lowercase without colons */
  for (index = 0; FINGERPRINT[index]; index += 1) {
    if (FINGERPRINT[index] != ':') pin[out++] = (char)(FINGERPRINT[index] >= 'A' && FINGERPRINT[index] <= 'F' ? FINGERPRINT[index] + 32 : FINGERPRINT[index]);
  }
  pin[out] = '\0';
  {
    channel_c_quic_tls_options_t tls = { NULL, pin, NULL };
    client = connect_quic(channel, url, &tls, &error);
    if (error) fprintf(stderr, "pinned: %s\n", error);
    assert_non_null(client);
    expect(channel_c_quic_client_sender(client), "hello", NULL, "\"world\"");
    channel_c_quic_client_free(client);
  }
  /* Pinned with colons: the CA doesn't matter then */
  {
    channel_c_quic_tls_options_t tls = { NULL, FINGERPRINT, "example.com" };
    client = connect_quic(channel, url, &tls, &error);
    assert_non_null(client);
    channel_c_quic_client_free(client);
  }
  /* CA, by IP address and by server name */
  {
    channel_c_quic_tls_options_t tls = { g_cert, NULL, "localhost" };
    client = connect_quic(channel, url, &tls, &error);
    if (error) fprintf(stderr, "server name: %s\n", error);
    assert_non_null(client);
    expect(channel_c_quic_client_sender(client), "hello", NULL, "\"world\"");
    channel_c_quic_client_free(client);
  }

  {
    channel_c_quic_tls_options_t system_roots = { NULL, NULL, NULL };
    channel_c_quic_tls_options_t wrong_pin = { NULL, "0000000000000000000000000000000000000000000000000000000000000000", NULL };
    channel_c_quic_tls_options_t wrong_name = { g_cert, NULL, "example.com" };
    channel_c_quic_tls_options_t invalid_pin = { NULL, "nope", NULL };
    channel_c_quic_tls_options_t ca = { g_cert, NULL, NULL };
    expect_refused(url, &system_roots, "system roots");
    expect_refused(url, &wrong_pin, "wrong pin");
    expect_refused(url, &wrong_name, "name not in cert");
    expect_refused(url, &invalid_pin, "invalid fingerprint");
    expect_refused("ws://127.0.0.1:1", &ca, "ws url");
    expect_refused("quic://127.0.0.1", &ca, "no port");
  }
  assert_int_equal(channel_c_quic_server_connection_count(server), 0);

  channel_c_quic_server_free(server);
  channel_c_channel_free(channel);
  free(url);
}

/* Raw QUIC connections, without the Channel client, to check the wire rules */

typedef struct {
  SSL_CTX *ctx;
  SSL *ssl;
  SSL *stream;
} raw_t;

static void raw_pump(raw_t *raw) {
  (void)SSL_handle_events(raw->ssl);
  (void)channel_c_internal_pump_all();
  ERR_clear_error();
  usleep(200);
}

static void raw_open(raw_t *raw, int port) {
  BIO_ADDR *address = BIO_ADDR_new();
  unsigned short port_n = htons((unsigned short)port);
  struct in_addr loopback;
  int64_t deadline;
  BIO *bio;
  int sock;

  memset(raw, 0, sizeof(*raw));
  loopback.s_addr = htonl(0x7f000001);
  assert_true(BIO_ADDR_rawmake(address, AF_INET, &loopback, sizeof(loopback), port_n));
  raw->ctx = SSL_CTX_new(OSSL_QUIC_client_method());
  SSL_CTX_set_verify(raw->ctx, SSL_VERIFY_NONE, NULL);
  raw->ssl = SSL_new(raw->ctx);
  assert_int_equal(SSL_set_alpn_protos(raw->ssl, (const unsigned char *)CHANNEL_C_QUIC_ALPN_WIRE, CHANNEL_C_QUIC_ALPN_WIRE_LENGTH), 0);
  sock = BIO_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 0);
  assert_true(sock >= 0);
  assert_true(BIO_connect(sock, address, 0));
  assert_true(BIO_socket_nbio(sock, 1));
  bio = BIO_new(BIO_s_datagram());
  BIO_set_fd(bio, sock, BIO_CLOSE);
  SSL_set_bio(raw->ssl, bio, bio);
  assert_true(SSL_set1_initial_peer_addr(raw->ssl, address));
  assert_true(SSL_set_default_stream_mode(raw->ssl, SSL_DEFAULT_STREAM_MODE_NONE));
  assert_true(SSL_set_blocking_mode(raw->ssl, 0));
  BIO_ADDR_free(address);

  deadline = channel_c_quic_now_ms() + 5000;
  while (SSL_connect(raw->ssl) != 1) {
    assert_true(channel_c_quic_now_ms() < deadline);
    raw_pump(raw);
  }
  raw->stream = SSL_new_stream(raw->ssl, 0);
  assert_non_null(raw->stream);
}

static void raw_close(raw_t *raw) {
  (void)SSL_shutdown_ex(raw->ssl, SSL_SHUTDOWN_FLAG_RAPID, NULL, 0);
  SSL_free(raw->stream);
  SSL_free(raw->ssl);
  SSL_CTX_free(raw->ctx);
  ERR_clear_error();
}

static void raw_write(raw_t *raw, SSL *stream, const void *data, size_t length) {
  size_t written = 0;
  int64_t deadline = channel_c_quic_now_ms() + 2000;
  while (!SSL_write_ex(stream, data, length, &written)) {
    assert_true(channel_c_quic_now_ms() < deadline);
    raw_pump(raw);
  }
  assert_int_equal(written, length);
}

static void raw_write_frame(raw_t *raw, const char *text) {
  channel_c_quic_buffer_t frame;
  memset(&frame, 0, sizeof(frame));
  assert_int_equal(channel_c_quic_buffer_append_frame(&frame, text, strlen(text)), 0);
  raw_write(raw, raw->stream, frame.data, channel_c_quic_buffer_length(&frame));
  channel_c_quic_buffer_free(&frame);
}

static int raw_closed(raw_t *raw, int milliseconds) {
  int64_t deadline = channel_c_quic_now_ms() + milliseconds;
  while (channel_c_quic_now_ms() < deadline) {
    SSL_CONN_CLOSE_INFO info;
    if (SSL_get_conn_close_info(raw->ssl, &info, sizeof(info))) return 1;
    raw_pump(raw);
  }
  return 0;
}

/* Reads frames until every id in [1, count] has an answer; bodies in `bodies` by id. */
static void raw_read_answers(raw_t *raw, char **bodies, int count) {
  channel_c_quic_frame_reader_t reader;
  int64_t deadline = channel_c_quic_now_ms() + 5000;
  int answered = 0;

  channel_c_quic_frame_reader_init(&reader, 0);
  while (answered < count) {
    char *text = NULL;
    assert_true(channel_c_quic_now_ms() < deadline);
    if (channel_c_quic_read(raw->stream, &reader) < 0) fail_msg("the stream ended");
    while (channel_c_quic_frame_reader_next(&reader, &text, NULL) == 1) {
      yyjson_doc *doc = yyjson_read(text, strlen(text), 0);
      yyjson_val *root = yyjson_doc_get_root(doc);
      yyjson_val *item;
      size_t index, max;
      yyjson_val *one[1];
      assert_non_null(doc);
      if (yyjson_is_obj(root)) {
        one[0] = root;
        for (index = 0; index < 1; index += 1) {
          int id = (int)yyjson_get_int(yyjson_obj_get(one[index], "id"));
          if (id >= 1 && id <= count && !bodies[id - 1]) {
            bodies[id - 1] = yyjson_val_write(yyjson_obj_get(one[index], "body"), 0, NULL);
            answered += 1;
          }
        }
      } else {
        yyjson_arr_foreach(root, index, max, item) {
          int id = (int)yyjson_get_int(yyjson_obj_get(item, "id"));
          if (id >= 1 && id <= count && !bodies[id - 1]) {
            bodies[id - 1] = yyjson_val_write(yyjson_obj_get(item, "body"), 0, NULL);
            answered += 1;
          }
        }
      }
      yyjson_doc_free(doc);
      free(text);
    }
    raw_pump(raw);
  }
  channel_c_quic_frame_reader_free(&reader);
}

static int stream_refused(SSL *stream) {
  int state = SSL_get_stream_write_state(stream);
  return state == SSL_STREAM_STATE_RESET_LOCAL || state == SSL_STREAM_STATE_RESET_REMOTE;
}

static void test_wire_rules(void **state) {
  channel_c_quic_server_t *server = listen_quic(0);
  int port = channel_c_quic_server_port(server);
  raw_t raw;
  (void)state;

  /* Raw frames: a hello with a non-string header, a keepalive, a batch */
  {
    char *bodies[2] = { NULL, NULL };
    unsigned char keepalive = 0;
    raw_open(&raw, port);
    raw_write_frame(&raw, "{\"hello\":{\"X-Name\":\"raw\",\"ignored\":1}}");
    raw_write(&raw, raw.stream, &keepalive, 1);
    raw_write_frame(&raw, "[{\"id\":1,\"path\":\"name\"},{\"id\":2,\"path\":\"hello\"}]");
    raw_read_answers(&raw, bodies, 2);
    {
      /* Both sides at the 30 s idle timeout */
      uint64_t idle = 0;
      assert_true(SSL_get_feature_negotiated_uint(raw.ssl, SSL_VALUE_QUIC_IDLE_TIMEOUT, &idle));
      assert_int_equal(idle, 30000);
    }
    assert_string_equal(bodies[0], "\"raw\"");
    assert_string_equal(bodies[1], "\"world\"");
    free(bodies[0]);
    free(bodies[1]);
    raw_close(&raw);
  }

  /* One stream per connection: a second bidirectional and a unidirectional stream are refused, the
   * first one keeps working */
  {
    char *bodies[1] = { NULL };
    SSL *second;
    SSL *uni;
    int64_t deadline;
    raw_open(&raw, port);
    raw_write_frame(&raw, "{\"hello\":{}}");
    pump_for(50);
    second = SSL_new_stream(raw.ssl, SSL_STREAM_FLAG_NO_BLOCK);
    uni = SSL_new_stream(raw.ssl, SSL_STREAM_FLAG_UNI | SSL_STREAM_FLAG_NO_BLOCK);
    assert_non_null(second);
    assert_non_null(uni);
    raw_write(&raw, second, "\x0c{\"hello\":{}}", 13);
    raw_write(&raw, uni, "\x0c{\"hello\":{}}", 13);
    deadline = channel_c_quic_now_ms() + 3000;
    /* The server's STOP_SENDING makes OpenSSL reset the sending part (RESET_LOCAL) */
    while (!stream_refused(second) || !stream_refused(uni)) {
      if (channel_c_quic_now_ms() > deadline) {
        fail_msg("extra streams not refused: write states %d, %d", SSL_get_stream_write_state(second), SSL_get_stream_write_state(uni));
      }
      {
        unsigned char sink[64];
        size_t got = 0;
        (void)SSL_read_ex(second, sink, sizeof(sink), &got);
      }
      raw_pump(&raw);
    }
    raw_write_frame(&raw, "{\"id\":1,\"path\":\"hello\"}");
    raw_read_answers(&raw, bodies, 1);
    assert_string_equal(bodies[0], "\"world\"");
    free(bodies[0]);
    assert_int_equal(channel_c_quic_server_connection_count(server), 1);
    SSL_free(second);
    SSL_free(uni);
    raw_close(&raw);
  }

  /* A first frame that isn't a hello closes */
  raw_open(&raw, port);
  raw_write_frame(&raw, "{\"id\":1,\"path\":\"hello\"}");
  assert_true(raw_closed(&raw, 3000));
  raw_close(&raw);

  /* Invalid JSON closes */
  raw_open(&raw, port);
  raw_write_frame(&raw, "{\"hello\":{}}");
  raw_write_frame(&raw, "{nope");
  assert_true(raw_closed(&raw, 3000));
  raw_close(&raw);

  /* A frame over the limit closes */
  raw_open(&raw, port);
  raw_write_frame(&raw, "{\"hello\":{}}");
  raw_write(&raw, raw.stream, "\x81\x00\x00\x01", 4);
  assert_true(raw_closed(&raw, 3000));
  raw_close(&raw);

  pump_for(50);
  channel_c_quic_server_free(server);
}

/* The TypeScript reference's QUIC client calls this server (tests/quic/client.ts) */
static void test_typescript_client(void **state) {
  channel_c_quic_server_t *server;
  char command[2048];
  char output[4096];
  size_t length = 0;
  FILE *pipe;
  int64_t deadline;
  int status;
  (void)state;

  if (system("command -v bun >/dev/null 2>&1") != 0) {
    fprintf(stderr, "bun isn't installed\n");
    skip();
  }
  g_connected = 0;
  g_disconnected = 0;
  server = listen_quic(0);
  snprintf(command, sizeof(command), "bun '%s/tests/quic/client.ts' %d '%s/tests/quic/cert.pem'",
           CHANNEL_C_SOURCE_DIR, channel_c_quic_server_port(server), CHANNEL_TS_DIR);
  pipe = popen(command, "r");
  assert_non_null(pipe);
  fcntl(fileno(pipe), F_SETFL, fcntl(fileno(pipe), F_GETFL) | O_NONBLOCK);
  deadline = channel_c_quic_now_ms() + 30000;
  for (;;) {
    ssize_t got = read(fileno(pipe), output + length, sizeof(output) - 1 - length);
    if (got > 0) {
      length += (size_t)got;
      continue;
    }
    if (got == 0) break;
    assert_true(channel_c_quic_now_ms() < deadline);
    if (!channel_c_quic_server_poll(server)) usleep(200);
  }
  output[length] = '\0';
  status = pclose(pipe);
  assert_int_equal(status, 0);
  while (length > 0 && (output[length - 1] == '\n' || output[length - 1] == '\r')) output[--length] = '\0';
  assert_string_equal(
    output,
    "{\"hello\":\"world\",\"echo\":{\"a\":[1,\"b\"]},\"name\":\"ts\",\"values\":[0,1,2],\"event\":\"tick\"}"
  );
  {
    int64_t wait_deadline = channel_c_quic_now_ms() + 5000;
    while (g_disconnected != g_connected && channel_c_quic_now_ms() < wait_deadline) {
      if (!channel_c_quic_server_poll(server)) usleep(200);
    }
  }
  assert_int_equal(g_connected, 1);
  assert_int_equal(g_disconnected, 1);
  channel_c_quic_server_free(server);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_client_and_server),
    cmocka_unit_test(test_certificate_trust),
    cmocka_unit_test(test_wire_rules),
    cmocka_unit_test(test_typescript_client),
  };
  return cmocka_run_group_tests(tests, setup, teardown);
}
