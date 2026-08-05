#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <cmocka.h>

#include "channel-c/channel_c.h"

static int g_values_sent = 0;

static int client_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"client world\"");
  return *out_body_json ? 0 : -1;
}

static void client_stream_values(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 3; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    assert_int_equal(channel_c_stream_emit_json(request->emitter, value), 0);
  }
}

static void client_stream_cancel(const channel_c_stream_request_t *request) {
  int i;
  for (i = 0; i < 10; i += 1) {
    char value[16];
    if (channel_c_stream_is_cancelled(request->emitter)) return;
    snprintf(value, sizeof(value), "%d", i);
    assert_int_equal(channel_c_stream_emit_json(request->emitter, value), 0);
    g_values_sent += 1;
  }
}

static void noop_event(const char *body_json, void *user_data) {
  (void)body_json;
  (void)user_data;
}

static int wait_remote_topic(channel_c_client_t *client, const char *topic, int expected, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (channel_c_client_remote_is_subscribed(client, topic) == expected) return 1;
    (void)channel_c_client_poll(client);
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static int wait_disconnected(channel_c_client_t *client, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    if (!channel_c_client_is_connected(client)) return 1;
    (void)channel_c_client_poll(client);
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static void test_client_interoperability_with_ts_server(void **state) {
  channel_c_channel_t *channel;
  channel_c_client_t *client;
  channel_c_client_t *client2;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_subscription_handle_t *handle = NULL;
  char *body = NULL;
  char *error = NULL;
  int done = 0;
  int index = 0;

  (void)state;

  g_values_sent = 0;
  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "hello", client_hello, NULL, 0), 0);
  assert_int_equal(channel_c_channel_stream(channel, "stream/values", client_stream_values, NULL), 0);
  assert_int_equal(channel_c_channel_stream(channel, "stream/cancel", client_stream_cancel, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(channel, "names", NULL, NULL), 0);

  client = channel_c_client_connect(channel, "ws://127.0.0.1:2049", NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  sender = channel_c_client_sender(client);
  assert_non_null(sender);

  assert_int_equal(channel_c_sender_send(sender, "hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "echo", "0.1337", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "0.1337");
  channel_c_free(body);
  body = NULL;

  assert_int_equal(channel_c_sender_send(sender, "empty", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_null(body);

  assert_int_not_equal(channel_c_sender_send(sender, "notfound", NULL, NULL, &body, &error), 0);
  assert_string_equal(error, "api not found");
  channel_c_free(error);
  error = NULL;

  assert_int_not_equal(
    channel_c_sender_subscribe(sender, "notfound", "\"1\"", NULL, noop_event, NULL, &handle, &error),
    0
  );
  assert_string_equal(error, "subscription not found");
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

  values = channel_c_sender_values(sender, "mirror/stream/cancel", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);
  done = 0;
  while (!done) {
    assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
    assert_null(error);
    if (done) break;
    if (strcmp(body, "2") == 0) {
      channel_c_free(body);
      body = NULL;
      break;
    }
    channel_c_free(body);
    body = NULL;
  }
  channel_c_values_close(values);
  assert_int_equal(g_values_sent, 10);

  assert_int_equal(channel_c_sender_send(sender, "mirror", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"client world\"");
  channel_c_free(body);
  body = NULL;

  values = channel_c_sender_values(sender, "mirror/stream", NULL, NULL, &error);
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

  assert_int_equal(channel_c_sender_send(sender, "mirror/events", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_null(body);
  assert_true(wait_remote_topic(client, "names/1", 1, 2000));
  assert_int_equal(channel_c_channel_event_send(channel, "names", "\"1\"", "\"h\"", &error), 0);
  assert_null(error);
  assert_true(wait_remote_topic(client, "names/1", 0, 2000));

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

  client2 = channel_c_client_connect(channel, "ws://127.0.0.1:2049", NULL, NULL, &error);
  assert_non_null(client2);
  assert_null(error);
  assert_int_not_equal(channel_c_client_send_string(client2, "auth/name", NULL, &body, &error), 0);
  assert_string_equal(error, "unauthorized");
  channel_c_free(error);
  error = NULL;
  channel_c_client_free(client2);

  (void)channel_c_sender_send(sender, "disconnect", NULL, NULL, &body, &error);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;
  assert_true(wait_disconnected(client, 2000));

  channel_c_client_free(client);
  channel_c_channel_free(channel);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_client_interoperability_with_ts_server),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
