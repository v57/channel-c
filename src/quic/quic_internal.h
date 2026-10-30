#ifndef CHANNEL_C_QUIC_INTERNAL_H
#define CHANNEL_C_QUIC_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <openssl/ssl.h>

#include "channel-c/channel_c_quic.h"
#include "internal.h"

/* ALPN wire form: length byte + "channel/1" */
#define CHANNEL_C_QUIC_ALPN_WIRE "\x09" CHANNEL_C_QUIC_ALPN
#define CHANNEL_C_QUIC_ALPN_WIRE_LENGTH 10u
/* Connections silent for 30 s are closed; clients send an empty frame every 15 s to stay open. */
#define CHANNEL_C_QUIC_IDLE_TIMEOUT_MS 30000u
#define CHANNEL_C_QUIC_KEEPALIVE_MS 15000

/* A byte queue: appended at the end, consumed from the front. */
typedef struct {
  unsigned char *data;
  size_t start;
  size_t end;
  size_t capacity;
} channel_c_quic_buffer_t;

int channel_c_quic_buffer_append(channel_c_quic_buffer_t *buffer, const void *data, size_t length);
void channel_c_quic_buffer_consume(channel_c_quic_buffer_t *buffer, size_t length);
size_t channel_c_quic_buffer_length(const channel_c_quic_buffer_t *buffer);
void channel_c_quic_buffer_free(channel_c_quic_buffer_t *buffer);

/* Appends one frame (varint length + payload) to `buffer`. 0 on success. */
int channel_c_quic_buffer_append_frame(channel_c_quic_buffer_t *buffer, const char *payload, size_t length);

/* Splits a byte stream back into frames, skipping keepalives. */
typedef struct {
  channel_c_quic_buffer_t buffer;
  size_t max_payload;
} channel_c_quic_frame_reader_t;

void channel_c_quic_frame_reader_init(channel_c_quic_frame_reader_t *reader, size_t max_payload);
int channel_c_quic_frame_reader_push(channel_c_quic_frame_reader_t *reader, const void *data, size_t length);
/* 1 with a NUL-terminated copy of the next frame in *out_text (free it), 0 when more bytes are
 * needed, -1 when a frame is over the limit or memory ran out. */
int channel_c_quic_frame_reader_next(channel_c_quic_frame_reader_t *reader, char **out_text, size_t *out_length);
void channel_c_quic_frame_reader_free(channel_c_quic_frame_reader_t *reader);

/* `{"hello":{name:value,...}}` from the client's handshake headers. */
char *channel_c_quic_hello_json(const channel_c_header_t *headers, size_t count);

/* Parses a hello frame into lowercased headers (string values only). 0 ok, -1 not a hello. */
int channel_c_quic_hello_parse(const char *text, size_t length, channel_c_header_t **out_items, size_t *out_count);
void channel_c_quic_headers_free(channel_c_header_t *items, size_t count);

/* Writes as much of `buffer` to `stream` as it takes. 0 ok (possibly partial), -1 on a stream error. */
int channel_c_quic_flush(SSL *stream, channel_c_quic_buffer_t *buffer);
/* Reads what `stream` has into `reader`. 1 read something, 0 nothing yet, -1 stream ended or failed. */
int channel_c_quic_read(SSL *stream, channel_c_quic_frame_reader_t *reader);

/* Last OpenSSL error as text (allocated), or `fallback`. */
char *channel_c_quic_ssl_error(const char *fallback);
int64_t channel_c_quic_now_ms(void);

#endif
