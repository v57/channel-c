#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <openssl/err.h>
#include <yyjson.h>

#include "quic_internal.h"

int64_t channel_c_quic_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64_t)ts.tv_sec * 1000LL) + ((int64_t)ts.tv_nsec / 1000000LL);
}

size_t channel_c_quic_varint_encode(uint64_t value, unsigned char out[8]) {
  if (value < (1u << 6)) {
    out[0] = (unsigned char)value;
    return 1;
  }
  if (value < (1u << 14)) {
    out[0] = (unsigned char)(0x40 | (value >> 8));
    out[1] = (unsigned char)value;
    return 2;
  }
  if (value < (1u << 30)) {
    out[0] = (unsigned char)(0x80 | (value >> 24));
    out[1] = (unsigned char)(value >> 16);
    out[2] = (unsigned char)(value >> 8);
    out[3] = (unsigned char)value;
    return 4;
  }
  out[0] = (unsigned char)(0xc0 | ((value >> 56) & 0x3f));
  out[1] = (unsigned char)(value >> 48);
  out[2] = (unsigned char)(value >> 40);
  out[3] = (unsigned char)(value >> 32);
  out[4] = (unsigned char)(value >> 24);
  out[5] = (unsigned char)(value >> 16);
  out[6] = (unsigned char)(value >> 8);
  out[7] = (unsigned char)value;
  return 8;
}

size_t channel_c_quic_buffer_length(const channel_c_quic_buffer_t *buffer) {
  return buffer ? buffer->end - buffer->start : 0;
}

int channel_c_quic_buffer_append(channel_c_quic_buffer_t *buffer, const void *data, size_t length) {
  if (!buffer) return -1;
  if (length == 0) return 0;
  if (buffer->capacity - buffer->end < length) {
    size_t used = buffer->end - buffer->start;
    if (buffer->start > 0) {
      memmove(buffer->data, buffer->data + buffer->start, used);
      buffer->start = 0;
      buffer->end = used;
    }
    if (buffer->capacity - buffer->end < length) {
      size_t capacity = buffer->capacity ? buffer->capacity : 4096;
      unsigned char *data_copy;
      while (capacity - used < length) {
        if (capacity > SIZE_MAX / 2) return -1;
        capacity *= 2;
      }
      data_copy = (unsigned char *)realloc(buffer->data, capacity);
      if (!data_copy) return -1;
      buffer->data = data_copy;
      buffer->capacity = capacity;
    }
  }
  memcpy(buffer->data + buffer->end, data, length);
  buffer->end += length;
  return 0;
}

void channel_c_quic_buffer_consume(channel_c_quic_buffer_t *buffer, size_t length) {
  if (!buffer) return;
  if (length >= buffer->end - buffer->start) {
    buffer->start = 0;
    buffer->end = 0;
  } else {
    buffer->start += length;
  }
}

void channel_c_quic_buffer_free(channel_c_quic_buffer_t *buffer) {
  if (!buffer) return;
  free(buffer->data);
  memset(buffer, 0, sizeof(*buffer));
}

int channel_c_quic_buffer_append_frame(channel_c_quic_buffer_t *buffer, const char *payload, size_t length) {
  unsigned char header[8];
  size_t header_length = channel_c_quic_varint_encode((uint64_t)length, header);
  if (channel_c_quic_buffer_append(buffer, header, header_length) != 0) return -1;
  return channel_c_quic_buffer_append(buffer, payload, length);
}

void channel_c_quic_frame_reader_init(channel_c_quic_frame_reader_t *reader, size_t max_payload) {
  memset(reader, 0, sizeof(*reader));
  reader->max_payload = max_payload ? max_payload : CHANNEL_C_MAX_MESSAGE_SIZE;
}

int channel_c_quic_frame_reader_push(channel_c_quic_frame_reader_t *reader, const void *data, size_t length) {
  return channel_c_quic_buffer_append(&reader->buffer, data, length);
}

int channel_c_quic_frame_reader_next(channel_c_quic_frame_reader_t *reader, char **out_text, size_t *out_length) {
  *out_text = NULL;
  if (out_length) *out_length = 0;
  for (;;) {
    const unsigned char *data = reader->buffer.data + reader->buffer.start;
    size_t available = channel_c_quic_buffer_length(&reader->buffer);
    size_t size;
    uint64_t length;
    size_t index;
    char *text;

    if (available == 0) return 0;
    size = (size_t)1 << (data[0] >> 6);
    if (available < size) return 0;
    length = data[0] & 0x3f;
    for (index = 1; index < size; index += 1) length = (length << 8) | data[index];
    /* Refused as soon as the length is known, before buffering the payload */
    if (length > reader->max_payload) return -1;
    if (available - size < length) return 0;
    if (length == 0) {
      /* Keepalive */
      channel_c_quic_buffer_consume(&reader->buffer, size);
      continue;
    }
    text = (char *)malloc((size_t)length + 1);
    if (!text) return -1;
    memcpy(text, data + size, (size_t)length);
    text[length] = '\0';
    channel_c_quic_buffer_consume(&reader->buffer, size + (size_t)length);
    *out_text = text;
    if (out_length) *out_length = (size_t)length;
    return 1;
  }
}

void channel_c_quic_frame_reader_free(channel_c_quic_frame_reader_t *reader) {
  if (!reader) return;
  channel_c_quic_buffer_free(&reader->buffer);
}

char *channel_c_quic_hello_json(const channel_c_header_t *headers, size_t count) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
  yyjson_mut_val *root;
  yyjson_mut_val *hello;
  size_t index;
  char *text;

  if (!doc) return NULL;
  root = yyjson_mut_obj(doc);
  hello = yyjson_mut_obj(doc);
  if (!root || !hello) {
    yyjson_mut_doc_free(doc);
    return NULL;
  }
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_val(doc, root, "hello", hello);
  for (index = 0; index < count; index += 1) {
    if (!headers[index].name || !headers[index].value) continue;
    yyjson_mut_obj_put(hello, yyjson_mut_strcpy(doc, headers[index].name), yyjson_mut_strcpy(doc, headers[index].value));
  }
  text = yyjson_mut_write(doc, 0, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

void channel_c_quic_headers_free(channel_c_header_t *items, size_t count) {
  size_t index;
  if (!items) return;
  for (index = 0; index < count; index += 1) {
    free((char *)items[index].name);
    free((char *)items[index].value);
  }
  free(items);
}

int channel_c_quic_hello_parse(const char *text, size_t length, channel_c_header_t **out_items, size_t *out_count) {
  yyjson_doc *doc;
  yyjson_val *hello;
  yyjson_val *key;
  yyjson_val *value;
  yyjson_obj_iter iter;
  channel_c_header_t *items;
  size_t count = 0;

  *out_items = NULL;
  *out_count = 0;
  doc = yyjson_read(text, length, 0);
  if (!doc) return -1;
  hello = yyjson_obj_get(yyjson_doc_get_root(doc), "hello");
  if (!yyjson_is_obj(hello)) {
    yyjson_doc_free(doc);
    return -1;
  }
  items = (channel_c_header_t *)calloc(yyjson_obj_size(hello) + 1, sizeof(channel_c_header_t));
  if (!items) {
    yyjson_doc_free(doc);
    return -1;
  }
  yyjson_obj_iter_init(hello, &iter);
  while ((key = yyjson_obj_iter_next(&iter))) {
    const char *name = yyjson_get_str(key);
    size_t name_length = yyjson_get_len(key);
    char *lower;
    char *copy;
    size_t index;
    size_t existing;

    value = yyjson_obj_iter_get_val(key);
    if (!yyjson_is_str(value)) continue;
    lower = (char *)malloc(name_length + 1);
    copy = channel_c_strdup(yyjson_get_str(value));
    if (!lower || !copy) {
      free(lower);
      free(copy);
      channel_c_quic_headers_free(items, count);
      yyjson_doc_free(doc);
      return -1;
    }
    for (index = 0; index < name_length; index += 1) lower[index] = (char)tolower((unsigned char)name[index]);
    lower[name_length] = '\0';
    /* A later name that lowercases the same wins */
    for (existing = 0; existing < count; existing += 1) {
      if (strcmp(items[existing].name, lower) == 0) break;
    }
    if (existing < count) {
      free(lower);
      free((char *)items[existing].value);
      items[existing].value = copy;
    } else {
      items[count].name = lower;
      items[count].value = copy;
      count += 1;
    }
  }
  yyjson_doc_free(doc);
  *out_items = items;
  *out_count = count;
  return 0;
}

int channel_c_quic_flush(SSL *stream, channel_c_quic_buffer_t *buffer) {
  while (channel_c_quic_buffer_length(buffer) > 0) {
    size_t written = 0;
    if (SSL_write_ex(stream, buffer->data + buffer->start, channel_c_quic_buffer_length(buffer), &written)) {
      channel_c_quic_buffer_consume(buffer, written);
      continue;
    }
    switch (SSL_get_error(stream, 0)) {
      case SSL_ERROR_WANT_READ:
      case SSL_ERROR_WANT_WRITE:
        ERR_clear_error();
        return 0;
      default:
        ERR_clear_error();
        return -1;
    }
  }
  return 0;
}

int channel_c_quic_read(SSL *stream, channel_c_quic_frame_reader_t *reader) {
  unsigned char chunk[65536];
  int progressed = 0;

  for (;;) {
    size_t read_bytes = 0;
    if (SSL_read_ex(stream, chunk, sizeof(chunk), &read_bytes)) {
      if (channel_c_quic_frame_reader_push(reader, chunk, read_bytes) != 0) return -1;
      progressed = 1;
      continue;
    }
    switch (SSL_get_error(stream, 0)) {
      case SSL_ERROR_WANT_READ:
      case SSL_ERROR_WANT_WRITE:
        ERR_clear_error();
        return progressed;
      default:
        /* Stream finished or reset, or the connection closed */
        ERR_clear_error();
        return progressed ? 1 : -1;
    }
  }
}

char *channel_c_quic_ssl_error(const char *fallback) {
  unsigned long code = ERR_peek_last_error();
  char text[256];
  if (!code) return channel_c_strdup(fallback);
  ERR_error_string_n(code, text, sizeof(text));
  ERR_clear_error();
  return channel_c_strdup(text);
}
