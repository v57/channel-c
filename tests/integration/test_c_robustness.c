/* Regression tests for AUDIT.md section 4 (server state factory headers, connect/disconnect hooks,
 * client headers) and AUDIT2.md section 3 (error-object replies) plus the 16 MiB message cap. */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmocka.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include "channel-c/channel_c.h"

/* ---------------------------------------------------------------------------------------------
 * Server state from headers + hooks
 * ------------------------------------------------------------------------------------------- */

typedef struct {
  char *name;
  char *only_query;
  char *authorization;
} header_state_t;

typedef struct {
  int connects;
  int disconnects;
  int saw_state_on_connect;
} hook_counts_t;

static char *dup_or_null(const char *text) {
  return text ? channel_c_strdup(text) : NULL;
}

static void *make_header_state(const channel_c_headers_t *headers, void *user_data) {
  header_state_t *state = (header_state_t *)calloc(1, sizeof(header_state_t));
  (void)user_data;
  if (!state) return NULL;
  state->name = dup_or_null(channel_c_headers_get(headers, "name"));
  state->only_query = dup_or_null(channel_c_headers_get(headers, "only"));
  state->authorization = dup_or_null(channel_c_headers_get(headers, "Authorization"));
  return state;
}

static void free_header_state(void *raw, void *user_data) {
  header_state_t *state = (header_state_t *)raw;
  (void)user_data;
  if (!state) return;
  free(state->name);
  free(state->only_query);
  free(state->authorization);
  free(state);
}

static void on_connect_hook(void *state, channel_c_sender_t *sender, void *user_data) {
  hook_counts_t *counts = (hook_counts_t *)user_data;
  counts->connects += 1;
  if (state && sender) counts->saw_state_on_connect = 1;
}

static void on_disconnect_hook(void *state, channel_c_sender_t *sender, void *user_data) {
  hook_counts_t *counts = (hook_counts_t *)user_data;
  (void)state;
  (void)sender;
  counts->disconnects += 1;
}

static int whoami(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  header_state_t *state = (header_state_t *)request->state;
  char buffer[512];
  (void)out_error;
  snprintf(
    buffer,
    sizeof(buffer),
    "\"%s|%s|%s\"",
    state && state->name ? state->name : "-",
    state && state->only_query ? state->only_query : "-",
    state && state->authorization ? state->authorization : "-"
  );
  *out_body_json = channel_c_strdup(buffer);
  return *out_body_json ? 0 : -1;
}

static void test_headers_reach_state_factory_and_hooks_run(void **unused) {
  channel_c_channel_t *server_channel = channel_c_channel_new();
  channel_c_channel_t *client_channel = channel_c_channel_new();
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_client_options_t options;
  hook_counts_t counts = { 0, 0, 0 };
  channel_c_header_t headers[] = {
    { "name", "header" },
    { "Authorization", "Bearer secret" },
  };
  char url[128];
  char *body = NULL;
  char *error = NULL;
  int waited;

  (void)unused;
  assert_int_equal(channel_c_channel_post(server_channel, "whoami", whoami, NULL, 0), 0);
  server = channel_c_server_listen(
    server_channel,
    "127.0.0.1",
    0,
    (channel_c_state_factory_t){
      .destroy = free_header_state,
      .user_data = &counts,
      .create_with_headers = make_header_state,
      .on_connect = on_connect_hook,
      .on_disconnect = on_disconnect_hook,
    },
    &error
  );
  assert_non_null(server);

  snprintf(url, sizeof(url), "ws://127.0.0.1:%d/?name=query&only=q", channel_c_server_port(server));
  memset(&options, 0, sizeof(options));
  options.headers = headers;
  options.header_count = 2;
  client = channel_c_client_connect_with_options(client_channel, url, &options, &error);
  assert_non_null(client);
  assert_null(error);

  assert_int_equal(channel_c_sender_send(channel_c_client_sender(client), "whoami", NULL, NULL, &body, &error), 0);
  assert_null(error);
  /* headers win over query params; query-only keys still arrive; names are case-insensitive */
  assert_string_equal(body, "\"header|q|Bearer secret\"");
  channel_c_free(body);
  assert_int_equal(counts.connects, 1);
  assert_true(counts.saw_state_on_connect);

  channel_c_client_free(client);
  for (waited = 0; waited < 2000 && counts.disconnects == 0; waited += 1) {
    (void)channel_c_server_poll(server);
    usleep(1000);
  }
  assert_int_equal(counts.disconnects, 1);

  channel_c_server_free(server);
  assert_int_equal(counts.disconnects, 1);
  channel_c_channel_free(client_channel);
  channel_c_channel_free(server_channel);
}

static void test_invalid_client_header_is_rejected(void **unused) {
  channel_c_channel_t *channel = channel_c_channel_new();
  channel_c_client_options_t options;
  channel_c_header_t bad[] = { { "x-evil", "a\r\nInjected: 1" } };
  char *error = NULL;

  (void)unused;
  memset(&options, 0, sizeof(options));
  options.headers = bad;
  options.header_count = 1;
  assert_null(channel_c_client_connect_with_options(channel, "ws://127.0.0.1:1/", &options, &error));
  assert_string_equal(error, "invalid header");
  channel_c_free(error);
  channel_c_channel_free(channel);
}

/* ---------------------------------------------------------------------------------------------
 * Hostile raw WebSocket server
 * ------------------------------------------------------------------------------------------- */

typedef struct {
  int listener;
  int port;
  const unsigned char *reply;
  size_t reply_length;
  pthread_t thread;
} hostile_server_t;

static void write_all(int fd, const void *data, size_t length) {
  const unsigned char *cursor = (const unsigned char *)data;
  while (length > 0) {
    ssize_t written = write(fd, cursor, length);
    if (written <= 0) return;
    cursor += written;
    length -= (size_t)written;
  }
}

static void *hostile_run(void *raw) {
  hostile_server_t *hostile = (hostile_server_t *)raw;
  char buffer[8192];
  size_t length = 0;
  int fd = accept(hostile->listener, NULL, NULL);
  const char *key_start;
  const char *key_end;
  unsigned char hash[SHA_DIGEST_LENGTH];
  char accept_key[64];
  char response[256];
  char material[128];

  if (fd < 0) return NULL;
  buffer[0] = '\0';
  while (!strstr(buffer, "\r\n\r\n")) {
    ssize_t n = read(fd, buffer + length, sizeof(buffer) - 1 - length);
    if (n <= 0) goto done;
    length += (size_t)n;
    buffer[length] = '\0';
  }
  key_start = strcasestr(buffer, "sec-websocket-key:");
  if (!key_start) goto done;
  key_start += strlen("sec-websocket-key:");
  while (*key_start == ' ') key_start += 1;
  key_end = strstr(key_start, "\r\n");
  if (!key_end) goto done;
  snprintf(material, sizeof(material), "%.*s258EAFA5-E914-47DA-95CA-C5AB0DC85B11", (int)(key_end - key_start), key_start);
  SHA1((const unsigned char *)material, strlen(material), hash);
  EVP_EncodeBlock((unsigned char *)accept_key, hash, SHA_DIGEST_LENGTH);
  snprintf(
    response,
    sizeof(response),
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
    accept_key
  );
  write_all(fd, response, strlen(response));
  if (read(fd, buffer, sizeof(buffer)) <= 0) goto done; /* the client's request frame */
  write_all(fd, hostile->reply, hostile->reply_length);
  while (read(fd, buffer, sizeof(buffer)) > 0) {
  }
done:
  close(fd);
  return NULL;
}

static void hostile_start(hostile_server_t *hostile, const unsigned char *reply, size_t reply_length) {
  struct sockaddr_in address;
  socklen_t address_length = sizeof(address);
  int one = 1;

  memset(hostile, 0, sizeof(*hostile));
  hostile->reply = reply;
  hostile->reply_length = reply_length;
  hostile->listener = socket(AF_INET, SOCK_STREAM, 0);
  assert_true(hostile->listener >= 0);
  setsockopt(hostile->listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert_int_equal(bind(hostile->listener, (struct sockaddr *)&address, sizeof(address)), 0);
  assert_int_equal(listen(hostile->listener, 1), 0);
  assert_int_equal(getsockname(hostile->listener, (struct sockaddr *)&address, &address_length), 0);
  hostile->port = ntohs(address.sin_port);
  assert_int_equal(pthread_create(&hostile->thread, NULL, hostile_run, hostile), 0);
}

static void hostile_join(hostile_server_t *hostile) {
  pthread_join(hostile->thread, NULL);
  close(hostile->listener);
}

/* Sends one request to a hostile server answering with `reply`; returns the call's result code and
 * hands back body/error. */
static int call_hostile(const unsigned char *reply, size_t reply_length, char **out_body, char **out_error) {
  hostile_server_t hostile;
  channel_c_channel_t *channel = channel_c_channel_new();
  channel_c_client_t *client;
  char url[64];
  int result;

  hostile_start(&hostile, reply, reply_length);
  snprintf(url, sizeof(url), "ws://127.0.0.1:%d/", hostile.port);
  client = channel_c_client_connect(channel, url, NULL, NULL, out_error);
  assert_non_null(client);
  result = channel_c_sender_send(channel_c_client_sender(client), "echo", "5", NULL, out_body, out_error);
  channel_c_client_free(client);
  hostile_join(&hostile);
  channel_c_channel_free(channel);
  return result;
}

static size_t text_frame(unsigned char *out, const char *payload) {
  size_t length = strlen(payload);
  size_t header = 2;
  out[0] = 0x81;
  if (length < 126) {
    out[1] = (unsigned char)length;
  } else {
    out[1] = 126;
    out[2] = (unsigned char)(length >> 8);
    out[3] = (unsigned char)(length & 0xff);
    header = 4;
  }
  memcpy(out + header, payload, length);
  return header + length;
}

static void test_error_object_reply_is_an_error(void **unused) {
  unsigned char frame[256];
  size_t frame_length = text_frame(frame, "{\"id\":1,\"error\":{\"code\":500,\"message\":\"nope\"}}");
  char *body = NULL;
  char *error = NULL;

  (void)unused;
  assert_int_not_equal(call_hostile(frame, frame_length, &body, &error), 0);
  assert_null(body);
  assert_non_null(error);
  assert_non_null(strstr(error, "500"));
  channel_c_free(error);
}

static void test_oversized_reply_closes_instead_of_buffering(void **unused) {
  /* A frame announcing CHANNEL_C_MAX_MESSAGE_SIZE + 1 bytes followed by its first payload bytes. */
  unsigned char frame[10 + 64];
  uint64_t announced = (uint64_t)CHANNEL_C_MAX_MESSAGE_SIZE + 1;
  char *body = NULL;
  char *error = NULL;
  int index;

  (void)unused;
  frame[0] = 0x81;
  frame[1] = 127;
  for (index = 0; index < 8; index += 1) frame[2 + index] = (unsigned char)(announced >> (56 - 8 * index));
  memset(frame + 10, 'a', 64);
  assert_int_not_equal(call_hostile(frame, sizeof(frame), &body, &error), 0);
  assert_null(body);
  assert_non_null(error);
  channel_c_free(error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_headers_reach_state_factory_and_hooks_run),
    cmocka_unit_test(test_invalid_client_header_is_rejected),
    cmocka_unit_test(test_error_object_reply_is_an_error),
    cmocka_unit_test(test_oversized_reply_closes_instead_of_buffering),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
