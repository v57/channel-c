#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "quic_internal.h"

static void test_varints(void **state) {
  static const struct {
    uint64_t value;
    size_t size;
    unsigned char first;
  } cases[] = {
    { 0, 1, 0x00 },
    { 63, 1, 0x3f },
    { 64, 2, 0x40 },
    { 16383, 2, 0x7f },
    { 16384, 4, 0x80 },
    { 1073741823, 4, 0xbf },
    { 1073741824, 8, 0xc0 },
  };
  size_t index;
  (void)state;

  for (index = 0; index < sizeof(cases) / sizeof(cases[0]); index += 1) {
    unsigned char out[8];
    assert_int_equal(channel_c_quic_varint_encode(cases[index].value, out), cases[index].size);
    assert_int_equal(out[0], cases[index].first);
  }
  /* RFC 9000 appendix A.1 examples */
  {
    unsigned char out[8];
    assert_int_equal(channel_c_quic_varint_encode(15293, out), 2);
    assert_int_equal(out[0], 0x7b);
    assert_int_equal(out[1], 0xbd);
    assert_int_equal(channel_c_quic_varint_encode(494878333, out), 4);
    assert_memory_equal(out, "\x9d\x7f\x3e\x7d", 4);
  }
}

/* Every size round-trips with the right header size, and a keepalive in front is skipped. */
static void test_frames_round_trip_and_skip_keepalives(void **state) {
  static const size_t sizes[] = { 0, 1, 63, 64, 16383, 16384, 70000 };
  size_t index;
  (void)state;

  for (index = 0; index < sizeof(sizes) / sizeof(sizes[0]); index += 1) {
    size_t size = sizes[index];
    size_t want_header = size < 64 ? 1 : size < 16384 ? 2 : 4;
    channel_c_quic_buffer_t frames;
    channel_c_quic_frame_reader_t reader;
    unsigned char keepalive = 0;
    char *payload = (char *)malloc(size + 1);
    char *text = NULL;
    size_t length = 0;
    size_t offset;

    assert_non_null(payload);
    memset(payload, 'x', size);
    payload[size] = '\0';
    memset(&frames, 0, sizeof(frames));
    assert_int_equal(channel_c_quic_buffer_append(&frames, &keepalive, 1), 0);
    assert_int_equal(channel_c_quic_buffer_append_frame(&frames, payload, size), 0);
    assert_int_equal(channel_c_quic_buffer_length(&frames), 1 + want_header + size);
    assert_int_equal(channel_c_quic_buffer_append_frame(&frames, "next", 4), 0);

    /* Fed one byte at a time and all at once */
    channel_c_quic_frame_reader_init(&reader, 0);
    for (offset = 0; offset < channel_c_quic_buffer_length(&frames); offset += 1) {
      int result;
      assert_int_equal(channel_c_quic_frame_reader_push(&reader, frames.data + offset, 1), 0);
      result = channel_c_quic_frame_reader_next(&reader, &text, &length);
      assert_true(result >= 0);
      if (result == 1) {
        if (size > 0 && length == size) {
          assert_memory_equal(text, payload, size);
        } else {
          assert_string_equal(text, "next");
        }
        free(text);
      }
    }
    channel_c_quic_frame_reader_free(&reader);

    channel_c_quic_frame_reader_init(&reader, 0);
    assert_int_equal(channel_c_quic_frame_reader_push(&reader, frames.data, channel_c_quic_buffer_length(&frames)), 0);
    if (size > 0) {
      assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, &length), 1);
      assert_int_equal(length, size);
      assert_memory_equal(text, payload, size);
      free(text);
    }
    assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, &length), 1);
    assert_string_equal(text, "next");
    free(text);
    assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, &length), 0);
    assert_null(text);
    channel_c_quic_frame_reader_free(&reader);

    channel_c_quic_buffer_free(&frames);
    free(payload);
  }
}

static void test_eight_byte_lengths_and_the_limit(void **state) {
  static const unsigned char big[] = { 0xc0, 0, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o' };
  static const unsigned char over[] = { 0x80 | 0x01, 0, 0, 1 };
  channel_c_quic_frame_reader_t reader;
  channel_c_quic_buffer_t frames;
  char *text = NULL;
  (void)state;

  channel_c_quic_frame_reader_init(&reader, 0);
  assert_int_equal(channel_c_quic_frame_reader_push(&reader, big, sizeof(big)), 0);
  assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, NULL), 1);
  assert_string_equal(text, "hello");
  free(text);
  channel_c_quic_frame_reader_free(&reader);

  /* 16 MiB + 1 is refused as soon as its length arrives */
  channel_c_quic_frame_reader_init(&reader, 0);
  assert_int_equal(channel_c_quic_frame_reader_push(&reader, over, sizeof(over)), 0);
  assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, NULL), -1);
  channel_c_quic_frame_reader_free(&reader);

  /* A custom limit */
  memset(&frames, 0, sizeof(frames));
  assert_int_equal(channel_c_quic_buffer_append_frame(&frames, "01234567890", 11), 0);
  channel_c_quic_frame_reader_init(&reader, 10);
  assert_int_equal(channel_c_quic_frame_reader_push(&reader, frames.data, 1), 0);
  assert_int_equal(channel_c_quic_frame_reader_next(&reader, &text, NULL), -1);
  channel_c_quic_frame_reader_free(&reader);
  channel_c_quic_buffer_free(&frames);
}

static void test_hello(void **state) {
  channel_c_header_t headers[] = { { "X-Name", "c" }, { "Authorization", "Bearer t" } };
  channel_c_header_t *items = NULL;
  size_t count = 0;
  char *text;
  (void)state;

  text = channel_c_quic_hello_json(headers, 2);
  assert_non_null(text);
  assert_string_equal(text, "{\"hello\":{\"X-Name\":\"c\",\"Authorization\":\"Bearer t\"}}");
  free(text);
  text = channel_c_quic_hello_json(NULL, 0);
  assert_string_equal(text, "{\"hello\":{}}");
  free(text);

  /* Names lowercased, only string values kept */
  {
    const char *hello = "{\"hello\":{\"X-Name\":\"raw\",\"ignored\":1,\"x-name\":\"later\",\"A\":\"b\"}}";
    assert_int_equal(channel_c_quic_hello_parse(hello, strlen(hello), &items, &count), 0);
    assert_int_equal(count, 2);
    assert_string_equal(items[0].name, "x-name");
    assert_string_equal(items[0].value, "later");
    assert_string_equal(items[1].name, "a");
    channel_c_quic_headers_free(items, count);
  }
  assert_int_equal(channel_c_quic_hello_parse("{\"id\":1,\"path\":\"hello\"}", 23, &items, &count), -1);
  assert_int_equal(channel_c_quic_hello_parse("{nope", 5, &items, &count), -1);
  assert_int_equal(channel_c_quic_hello_parse("{\"hello\":1}", 11, &items, &count), -1);
}

int main(void) {
  const struct CMUnitTest tests[] = {
    cmocka_unit_test(test_varints),
    cmocka_unit_test(test_frames_round_trip_and_skip_keepalives),
    cmocka_unit_test(test_eight_byte_lengths_and_the_limit),
    cmocka_unit_test(test_hello),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}
