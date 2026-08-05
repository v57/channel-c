#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <cmocka.h>
#include <yyjson.h>

#include "channel-c/channel_c.h"
#include "../../src/core/internal.h"

typedef struct {
  char *name;
} auth_state_t;

static void free_auth_state(void *state, void *user_data) {
  auth_state_t *auth = (auth_state_t *)state;
  (void)user_data;
  if (!auth) return;
  free(auth->name);
  free(auth);
}

static void *make_auth_state(void *user_data) {
  (void)user_data;
  return calloc(1, sizeof(auth_state_t));
}

static int server_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static int server_echo(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_error;
  *out_body_json = request->body_json ? channel_c_strdup(request->body_json) : channel_c_strdup("null");
  return *out_body_json ? 0 : -1;
}

static int server_empty(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_body_json;
  (void)out_error;
  return 0;
}

static int server_disconnect(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_body_json;
  (void)out_error;
  channel_c_sender_stop(request->sender);
  return 0;
}

static int server_auth(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  auth_state_t *state = (auth_state_t *)request->state;
  yyjson_doc *doc;
  yyjson_val *root;
  yyjson_val *name;

  if (!state || !request->body_json) {
    *out_error = channel_c_strdup("unauthorized");
    return -1;
  }

  doc = yyjson_read(request->body_json, strlen(request->body_json), 0);
  if (!doc) {
    *out_error = channel_c_strdup("invalid body");
    return -1;
  }

  root = yyjson_doc_get_root(doc);
  name = yyjson_obj_get(root, "name");
  if (!yyjson_is_str(name)) {
    yyjson_doc_free(doc);
    *out_error = channel_c_strdup("invalid body");
    return -1;
  }

  free(state->name);
  state->name = channel_c_strdup(yyjson_get_str(name));
  yyjson_doc_free(doc);

  if (!state->name) {
    *out_error = channel_c_strdup("internal error");
    return -1;
  }

  *out_body_json = (char *)malloc(strlen(state->name) + 3);
  assert_non_null(*out_body_json);
  snprintf(*out_body_json, strlen(state->name) + 3, "\"%s\"", state->name);
  return 0;
}

static int server_auth_name(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  auth_state_t *state = (auth_state_t *)request->state;
  if (!state || !state->name) {
    *out_error = channel_c_strdup("unauthorized");
    return -1;
  }

  *out_body_json = (char *)malloc(strlen(state->name) + 3);
  assert_non_null(*out_body_json);
  snprintf(*out_body_json, strlen(state->name) + 3, "\"%s\"", state->name);
  return 0;
}

static void server_stream_values(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 3; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    assert_int_equal(channel_c_stream_emit_json(request->emitter, value), 0);
  }
}

static void on_event(const char *body_json, void *user_data) {
  int *count = (int *)user_data;
  if (body_json && strcmp(body_json, "\"hello\"") == 0) *count += 1;
}

static int wait_for_disconnect(channel_c_client_t *client, channel_c_server_t *server, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    (void)channel_c_client_poll(client);
    (void)channel_c_server_poll(server);
    if (!channel_c_client_is_connected(client) && channel_c_server_connection_count(server) == 0) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_for_event_count(
  channel_c_client_t *client,
  channel_c_server_t *server,
  int *count,
  int expected,
  int timeout_ms
) {
  int elapsed = 0;

  while (elapsed < timeout_ms) {
    (void)channel_c_client_poll(client);
    (void)channel_c_server_poll(server);
    if (count && *count == expected) return 1;
    usleep(1000);
    elapsed += 1;
  }

  return 0;
}

static int wait_for_remote_topic(
  channel_c_client_t *client,
  channel_c_server_t *server,
  channel_c_connection_t *connection,
  const char *topic,
  int expected,
  int timeout_ms
) {
  int elapsed = 0;

  while (elapsed < timeout_ms) {
    (void)channel_c_client_poll(client);
    (void)channel_c_server_poll(server);
    if (!!channel_c_connection_is_remote_subscribed(connection, topic) == !!expected) return 1;
    usleep(1000);
    elapsed += 1;
  }

  return 0;
}

static void test_same_process_server_client(void **state) {
  channel_c_channel_t *server_channel;
  channel_c_channel_t *client_channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_subscription_handle_t *handle = NULL;
  channel_c_connection_t *server_connection;
  char *url = NULL;
  char *body = NULL;
  char *error = NULL;
  int event_count = 0;
  int done = 0;
  int index = 0;
  int length;
  size_t send_index;

  (void)state;

  server_channel = channel_c_channel_new();
  client_channel = channel_c_channel_new();
  assert_non_null(server_channel);
  assert_non_null(client_channel);

  assert_int_equal(channel_c_channel_post(server_channel, "hello", server_hello, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "echo", server_echo, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "empty", server_empty, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "disconnect", server_disconnect, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "auth", server_auth, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(server_channel, "auth/name", server_auth_name, NULL, 0), 0);
  assert_int_equal(channel_c_channel_stream(server_channel, "stream/values", server_stream_values, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(server_channel, "names", NULL, NULL), 0);

  server = channel_c_server_listen(
    server_channel,
    "127.0.0.1",
    0,
    (channel_c_state_factory_t){ .create = make_auth_state, .destroy = free_auth_state, .user_data = NULL },
    &error
  );
  assert_non_null(server);
  assert_null(error);
  assert_true(channel_c_server_port(server) > 0);

  length = snprintf(NULL, 0, "ws://127.0.0.1:%d", channel_c_server_port(server));
  url = (char *)malloc((size_t)length + 1);
  assert_non_null(url);
  snprintf(url, (size_t)length + 1, "ws://127.0.0.1:%d", channel_c_server_port(server));

  client = channel_c_client_connect(client_channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  sender = channel_c_client_sender(client);
  assert_non_null(sender);
  assert_true(channel_c_client_is_connected(client));
  assert_int_equal(channel_c_server_connection_count(server), 1);

  assert_int_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "echo", "42", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "42");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "empty", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_null(body);

  assert_int_not_equal(channel_c_sender_send(sender, "missing", NULL, NULL, &body, &error), 0);
  assert_string_equal(error, "api not found");
  channel_c_free(error);
  error = NULL;

  values = channel_c_sender_values(sender, "stream/values", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  done = 0;
  index = 0;
  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (!done) {
      char expected[16];
      snprintf(expected, sizeof(expected), "%d", index++);
      assert_string_equal(body, expected);
      channel_c_free(body);
      body = NULL;
    }
  }
  assert_int_equal(index, 3);
  channel_c_values_close(values);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "names", "\"1\"", NULL, on_event, &event_count, &handle, &error),
    0
  );
  assert_null(error);
  assert_non_null(handle);

  server_connection = channel_c_internal_server_first_connection(server);
  assert_non_null(server_connection);
  assert_true(wait_for_remote_topic(client, server, server_connection, "names/1", 1, 2000));

  assert_int_equal(channel_c_channel_event_send(server_channel, "names", "\"1\"", "\"hello\"", &error), 0);
  assert_null(error);
  assert_true(wait_for_event_count(client, server, &event_count, 1, 2000));

  channel_c_subscription_cancel(handle);
  handle = NULL;
  assert_true(wait_for_remote_topic(client, server, server_connection, "names/1", 0, 2000));

  assert_int_equal(channel_c_sender_send(sender, "auth", "{\"name\":\"tester\"}", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"tester\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "auth/name", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"tester\"");
  channel_c_free(body);
  body = NULL;

  for (send_index = 0; send_index < 256; send_index += 1) {
    char expected[32];
    snprintf(expected, sizeof(expected), "%zu", send_index);
    assert_int_equal(channel_c_sender_send(sender, "echo", expected, NULL, &body, &error), 0);
    assert_null(error);
    assert_string_equal(body, expected);
    channel_c_free(body);
    body = NULL;
  }

  (void)channel_c_sender_send(sender, "disconnect", NULL, NULL, &body, &error);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  assert_true(wait_for_disconnect(client, server, 5000));

  channel_c_client_free(client);
  channel_c_server_free(server);
  channel_c_channel_free(client_channel);
  channel_c_channel_free(server_channel);
  free(url);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_same_process_server_client),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
