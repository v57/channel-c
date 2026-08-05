#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <cmocka.h>

#include "channel-c/channel_c.h"

static int server_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static int server_disconnect(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)out_body_json;
  (void)out_error;
  channel_c_sender_stop(request->sender);
  return 0;
}

static int wait_for_disconnect(channel_c_client_t *client, channel_c_server_t *server, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    int client_progress = channel_c_client_poll(client);
    int server_progress = channel_c_server_poll(server);
    if (client_progress < 0 || server_progress < 0) return 0;
    if (!channel_c_client_is_connected(client) && channel_c_server_connection_count(server) == 0) return 1;
    usleep(1000);
    elapsed += 1;
  }
  return 0;
}

static void test_invalid_arguments(void **state) {
  char *error = NULL;

  (void)state;

  assert_null(channel_c_client_connect(NULL, "ws://127.0.0.1:1", NULL, NULL, &error));
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  assert_null(channel_c_server_listen(NULL, "127.0.0.1", 0, (channel_c_state_factory_t){0}, &error));
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
}

static void test_port_zero_poll_and_disconnect_cleanup(void **state) {
  channel_c_channel_t *channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  char *url = NULL;
  char *body = NULL;
  char *error = NULL;
  int length;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "hello", server_hello, NULL, 0), 0);
  assert_int_equal(channel_c_channel_post(channel, "disconnect", server_disconnect, NULL, 0), 0);

  server = channel_c_server_listen(channel, "127.0.0.1", 0, (channel_c_state_factory_t){0}, &error);
  assert_non_null(server);
  assert_null(error);
  assert_true(channel_c_server_port(server) > 0);

  length = snprintf(NULL, 0, "ws://127.0.0.1:%d", channel_c_server_port(server));
  url = (char *)malloc((size_t)length + 1);
  assert_non_null(url);
  snprintf(url, (size_t)length + 1, "ws://127.0.0.1:%d", channel_c_server_port(server));

  client = channel_c_client_connect(channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);
  assert_true(channel_c_client_is_connected(client));
  assert_int_equal(channel_c_server_connection_count(server), 1);

  assert_int_equal(channel_c_client_poll(client) >= 0, 1);
  assert_int_equal(channel_c_server_poll(server) >= 0, 1);

  assert_int_equal(channel_c_client_send_string(client, "hello", NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "world");
  channel_c_free(body);
  body = NULL;

  (void)channel_c_client_send_string(client, "disconnect", NULL, &body, &error);
  channel_c_free(body);
  channel_c_free(error);
  body = NULL;
  error = NULL;

  assert_true(wait_for_disconnect(client, server, 5000));

  channel_c_client_free(client);
  channel_c_server_free(server);
  channel_c_channel_free(channel);
  free(url);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_invalid_arguments),
    cmocka_unit_test(test_port_zero_poll_and_disconnect_cleanup),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
