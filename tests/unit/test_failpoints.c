#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <cmocka.h>

#include "channel-c/channel_c.h"
#include "../../src/core/internal.h"

static int server_hello(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static void test_process_creation_failpoint(void **state) {
  channel_c_channel_t *channel;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  channel_c_internal_failpoint_reset();
  channel_c_internal_failpoint_set("process.connection_create_fail", 1);
  assert_null(channel_c_process_new(channel, NULL, NULL));
  channel_c_internal_failpoint_reset();
  channel_c_channel_free(channel);
}

static void test_server_creation_failpoint(void **state) {
  channel_c_channel_t *channel;
  char *error = NULL;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  channel_c_internal_failpoint_reset();
  channel_c_internal_failpoint_set("server.context_create_fail", 1);
  assert_null(channel_c_server_listen(channel, "127.0.0.1", 0, (channel_c_state_factory_t){0}, &error));
  assert_string_equal(error, "failed to create websocket server context");
  channel_c_free(error);
  channel_c_internal_failpoint_reset();
  channel_c_channel_free(channel);
}

static void test_client_write_failpoint_closes_connection(void **state) {
  channel_c_channel_t *channel;
  channel_c_server_t *server;
  channel_c_client_t *client;
  char *url = NULL;
  char *error = NULL;
  char *body = NULL;
  int length;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_post(channel, "hello", server_hello, NULL, 0), 0);

  server = channel_c_server_listen(channel, "127.0.0.1", 0, (channel_c_state_factory_t){0}, &error);
  assert_non_null(server);
  assert_null(error);

  length = snprintf(NULL, 0, "ws://127.0.0.1:%d", channel_c_server_port(server));
  url = (char *)malloc((size_t)length + 1);
  assert_non_null(url);
  snprintf(url, (size_t)length + 1, "ws://127.0.0.1:%d", channel_c_server_port(server));

  client = channel_c_client_connect(channel, url, NULL, NULL, &error);
  assert_non_null(client);
  assert_null(error);

  channel_c_internal_failpoint_reset();
  channel_c_internal_failpoint_set("client.write_fail", 1);
  assert_int_not_equal(channel_c_client_send_string(client, "hello", NULL, &body, &error), 0);
  assert_non_null(error);
  channel_c_free(error);
  error = NULL;
  channel_c_free(body);
  body = NULL;
  channel_c_internal_failpoint_reset();

  assert_false(channel_c_client_is_connected(client));

  channel_c_client_free(client);
  channel_c_server_free(server);
  channel_c_channel_free(channel);
  free(url);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_process_creation_failpoint),
    cmocka_unit_test(test_server_creation_failpoint),
    cmocka_unit_test(test_client_write_failpoint_closes_connection),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
