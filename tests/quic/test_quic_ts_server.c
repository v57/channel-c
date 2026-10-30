/* The C QUIC client against the TypeScript reference's fixture (channel/tests/server.ts), which serves
 * its routes over QUIC on UDP next to WebSocket on TCP, same port: CHANNEL_TEST_PORT, default 2049. */
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "channel-c/channel_c_quic.h"

#define FINGERPRINT "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"

static char g_url[64];
static int g_port;

static long now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

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

/* The fixture's WebSocket port answers when the fixture runs */
static int fixture_running(void) {
  struct sockaddr_in address;
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  int ok;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons((unsigned short)g_port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ok = sock >= 0 && connect(sock, (struct sockaddr *)&address, sizeof(address)) == 0;
  if (sock >= 0) close(sock);
  return ok;
}

static int client_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"client world\"");
  return 0;
}

static void client_stream_values(const channel_c_stream_request_t *request) {
  int index;
  for (index = 0; index < 3; index += 1) {
    char value[16];
    snprintf(value, sizeof(value), "%d", index);
    if (channel_c_stream_emit_json(request->emitter, value) != 0) return;
  }
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

static int wait_remote_topic(channel_c_quic_client_t *client, const char *topic, int expected) {
  long deadline = now_ms() + 3000;
  while (now_ms() < deadline) {
    if (!!channel_c_quic_client_remote_is_subscribed(client, topic) == expected) return 1;
    if (!channel_c_quic_client_poll(client)) usleep(500);
  }
  return 0;
}

static void test_interop_with_ts_server(void **state) {
  char *cert = read_file(CHANNEL_TS_DIR "/tests/quic/cert.pem");
  channel_c_header_t headers[] = { { "X-Port", "c" } };
  channel_c_client_options_t options;
  channel_c_quic_tls_options_t tls = { NULL, NULL, NULL };
  channel_c_channel_t *channel;
  channel_c_quic_client_t *client;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  char *body = NULL;
  char *error = NULL;
  int done = 0;
  int index;
  long deadline;
  (void)state;

  assert_non_null(cert);
  channel = channel_c_channel_new();
  channel_c_channel_post(channel, "hello", client_hello, NULL, 0);
  channel_c_channel_stream(channel, "stream/values", client_stream_values, NULL);
  channel_c_channel_subscription(channel, "names", NULL, NULL);

  memset(&options, 0, sizeof(options));
  options.headers = headers;
  options.header_count = 1;
  tls.ca_pem = cert;
  client = channel_c_quic_client_connect(channel, g_url, &options, &tls, &error);
  if (error) fprintf(stderr, "connect %s: %s\n", g_url, error);
  assert_non_null(client);
  sender = channel_c_quic_client_sender(client);

  expect(sender, "hello", NULL, "\"world\"");
  expect(sender, "echo", "{\"a\":[1,\"b\"]}", "{\"a\":[1,\"b\"]}");
  expect(sender, "empty", NULL, NULL);
  /* The fixture calls this client back */
  expect(sender, "mirror", NULL, "\"client world\"");
  expect(sender, "auth", "{\"name\":\"quic\"}", "\"quic\"");
  expect(sender, "auth/name", NULL, "\"quic\"");

  /* The hello headers, lowercased, are the connection's headers */
  assert_int_equal(channel_c_sender_send(sender, "v2/headers", NULL, NULL, &body, &error), 0);
  assert_non_null(body);
  assert_non_null(strstr(body, "\"x-port\":\"c\""));
  channel_c_free(body);
  body = NULL;

  values = channel_c_sender_values(sender, "mirror/stream", NULL, NULL, &error);
  assert_non_null(values);
  for (index = 0; !done; index += 1) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (!done) {
      char want[8];
      snprintf(want, sizeof(want), "%d", index);
      assert_string_equal(body, want);
      channel_c_free(body);
      body = NULL;
    }
  }
  assert_int_equal(index, 4);
  channel_c_values_close(values);

  /* The fixture subscribes to this client's names/1 and cancels after the first event */
  expect(sender, "mirror/events", NULL, NULL);
  assert_true(wait_remote_topic(client, "names/1", 1));
  assert_int_equal(channel_c_channel_event_send(channel, "names", "\"1\"", "\"h\"", &error), 0);
  assert_true(wait_remote_topic(client, "names/1", 0));

  assert_int_not_equal(channel_c_sender_send(sender, "v2/error/code", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;

  /* The fixture drops the connection without answering: the request fails, as over WebSocket
   * (this port's clients don't reconnect) */
  expect(sender, "v2/drops/reset", NULL, NULL);
  assert_int_not_equal(channel_c_sender_send(sender, "v2/drop", NULL, NULL, &body, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;
  deadline = now_ms() + 3000;
  while (channel_c_quic_client_is_connected(client) && now_ms() < deadline) {
    if (!channel_c_quic_client_poll(client)) usleep(500);
  }
  assert_false(channel_c_quic_client_is_connected(client));
  channel_c_quic_client_free(client);

  /* A pinned fingerprint */
  tls.ca_pem = NULL;
  tls.fingerprint = FINGERPRINT;
  client = channel_c_quic_client_connect(channel, g_url, NULL, &tls, &error);
  if (error) fprintf(stderr, "pinned: %s\n", error);
  assert_non_null(client);
  expect(channel_c_quic_client_sender(client), "v2/drops", NULL, "1");
  channel_c_quic_client_free(client);

  channel_c_channel_free(channel);
  free(cert);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_interop_with_ts_server),
  };
  const char *port = getenv("CHANNEL_TEST_PORT");
  g_port = port && port[0] ? atoi(port) : 2049;
  snprintf(g_url, sizeof(g_url), "quic://127.0.0.1:%d", g_port);
  if (!fixture_running()) {
    fprintf(stderr, "the TypeScript fixture isn't running on 127.0.0.1:%d, skipping\n", g_port);
    return 0;
  }
  return cmocka_run_group_tests(tests, NULL, NULL);
}
