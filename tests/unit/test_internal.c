#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "channel-c/channel_c.h"

static int hello_post(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  (void)request;
  (void)out_error;
  *out_body_json = channel_c_strdup("\"world\"");
  return *out_body_json ? 0 : -1;
}

static void numbers_stream(const channel_c_stream_request_t *request) {
  assert_int_equal(channel_c_stream_emit_json(request->emitter, "1"), 0);
  assert_int_equal(channel_c_stream_emit_json(request->emitter, "2"), 0);
}

static void on_event(const char *body_json, void *user_data) {
  int *count = (int *)user_data;
  if (body_json && strcmp(body_json, "\"hello\"") == 0) *count += 1;
}

static void test_merge_prefixes_posts_streams_and_subscriptions(void **state) {
  channel_c_channel_t *destination;
  channel_c_channel_t *source;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  channel_c_values_t *values;
  channel_c_subscription_handle_t *handle = NULL;
  char *body = NULL;
  char *error = NULL;
  int done = 0;
  int event_count = 0;

  (void)state;

  destination = channel_c_channel_new();
  source = channel_c_channel_new();
  assert_non_null(destination);
  assert_non_null(source);

  assert_int_equal(channel_c_channel_post(source, "hello", hello_post, NULL, 0), 0);
  assert_int_equal(channel_c_channel_stream(source, "numbers", numbers_stream, NULL), 0);
  assert_int_equal(channel_c_channel_subscription(source, "updates", NULL, NULL), 0);
  assert_int_equal(channel_c_channel_merge(destination, source, "merged"), 0);

  process = channel_c_process_new(destination, NULL, NULL);
  assert_non_null(process);
  sender = channel_c_process_sender(process);

  assert_int_equal(channel_c_sender_send(sender, "merged/hello", NULL, NULL, &body, &error), 0);
  assert_null(error);
  assert_string_equal(body, "\"world\"");
  channel_c_free(body);
  body = NULL;

  values = channel_c_sender_values(sender, "merged/numbers", NULL, NULL, &error);
  assert_non_null(values);
  assert_null(error);

  assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_string_equal(body, "1");
  channel_c_free(body);
  body = NULL;
  assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_string_equal(body, "2");
  channel_c_free(body);
  body = NULL;
  assert_int_equal(channel_c_values_next(values, &body, &done, &error), 0);
  assert_true(done);
  channel_c_values_close(values);

  assert_int_equal(
    channel_c_sender_subscribe(sender, "merged/updates", "\"1\"", NULL, on_event, &event_count, &handle, &error),
    0
  );
  assert_null(error);
  assert_non_null(handle);

  assert_int_equal(channel_c_channel_event_send(destination, "merged/updates", "\"1\"", "\"hello\"", &error), 0);
  assert_null(error);
  assert_int_equal(event_count, 1);

  channel_c_subscription_cancel(handle);
  channel_c_process_free(process);
  channel_c_channel_free(source);
  channel_c_channel_free(destination);
}

static void test_event_send_validates_arguments(void **state) {
  channel_c_channel_t *channel;
  char *error = NULL;

  (void)state;

  channel = channel_c_channel_new();
  assert_non_null(channel);
  assert_int_equal(channel_c_channel_subscription(channel, "updates", NULL, NULL), 0);

  assert_int_not_equal(channel_c_channel_event_send(NULL, "updates", "\"1\"", "\"hello\"", &error), 0);
  assert_string_equal(error, "invalid arguments");
  channel_c_free(error);
  error = NULL;

  assert_int_not_equal(channel_c_channel_event_send(channel, "missing", "\"1\"", "\"hello\"", &error), 0);
  assert_string_equal(error, "subscription not found");
  channel_c_free(error);
  error = NULL;

  assert_int_not_equal(channel_c_channel_event_send(channel, "updates", "\"1\"", NULL, &error), 0);
  assert_string_equal(error, "body is required");
  channel_c_free(error);

  channel_c_channel_free(channel);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_merge_prefixes_posts_streams_and_subscriptions),
    cmocka_unit_test(test_event_send_validates_arguments),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}
