#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <yyjson.h>

#include "channel-c/channel_c.h"

#define BURST_BATCH_SIZE 2048
#define DEFAULT_MAX_IN_FLIGHT 4096
#define REQUEST_PATH "text/count"
#define REQUEST_BODY_TEXT "hello"

typedef enum {
  REQUEST_MODE_BURST = 0,
  REQUEST_MODE_STREAM = 1,
} request_mode_t;

typedef struct {
  int fd;
  unsigned char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;
  uint32_t mask_seed;
} raw_burst_client_t;

static volatile sig_atomic_t g_stop_requested = 0;
static uint64_t g_server_requests = 0;

static int64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64_t)ts.tv_sec * 1000000000LL) + (int64_t)ts.tv_nsec;
}

static double nanos_to_seconds(int64_t value) {
  return (double)value / 1000000000.0;
}

static void on_signal(int signum) {
  (void)signum;
  g_stop_requested = 1;
}

static void install_signal_handlers(void) {
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  signal(SIGPIPE, SIG_IGN);
}

static int read_positive_env(const char *name, int default_value) {
  const char *raw = getenv(name);
  char *end = NULL;
  long value;

  if (!raw || !raw[0]) return default_value;
  value = strtol(raw, &end, 10);
  if (!end || *end != '\0' || value <= 0 || value > INT32_MAX) return default_value;
  return (int)value;
}

static int read_non_negative_env(const char *name, int default_value) {
  const char *raw = getenv(name);
  char *end = NULL;
  long value;

  if (!raw || !raw[0]) return default_value;
  value = strtol(raw, &end, 10);
  if (!end || *end != '\0' || value < 0 || value > INT32_MAX) return default_value;
  return (int)value;
}

static request_mode_t read_request_mode(void) {
  const char *raw = getenv("BENCH_REQUEST_MODE");
  if (!raw || !raw[0] || strcmp(raw, "burst") == 0) return REQUEST_MODE_BURST;
  if (strcmp(raw, "stream") == 0) return REQUEST_MODE_STREAM;
  return REQUEST_MODE_BURST;
}

static char *read_string_body(const char *json_text) {
  yyjson_doc *doc;
  yyjson_val *root;
  char *value = NULL;

  if (!json_text) return NULL;
  doc = yyjson_read(json_text, strlen(json_text), 0);
  if (!doc) return NULL;

  root = yyjson_doc_get_root(doc);
  if (root && yyjson_is_obj(root)) {
    yyjson_val *text = yyjson_obj_get(root, "text");
    if (yyjson_is_str(text)) value = channel_c_strdup(yyjson_get_str(text));
  }

  yyjson_doc_free(doc);
  return value;
}

static int count_text(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  char *text;
  (void)out_error;

  text = read_string_body(request->body_json);
  if (!text) {
    *out_body_json = channel_c_strdup("0");
  } else {
    int length = (int)strlen(text);
    *out_body_json = (char *)malloc(32);
    if (*out_body_json) snprintf(*out_body_json, 32, "%d", length);
    free(text);
  }

  if (!*out_body_json) {
    *out_error = channel_c_strdup("internal error");
    return -1;
  }

  g_server_requests += 1;
  return 0;
}

static int count_text_process(const channel_c_post_request_t *request, char **out_body_json, char **out_error) {
  int length = request->body_json ? (int)strlen(request->body_json) : 0;

  (void)out_error;

  *out_body_json = (char *)malloc(32);
  if (!*out_body_json) {
    *out_error = channel_c_strdup("internal error");
    return -1;
  }

  snprintf(*out_body_json, 32, "%d", length);
  return 0;
}

static void emit_ready_server(int port) {
  printf("{\"type\":\"ready\",\"pid\":%d,\"port\":%d}\n", getpid(), port);
  fflush(stdout);
}

static void emit_ready_client(int clients) {
  printf("{\"type\":\"ready\",\"pid\":%d,\"clients\":%d}\n", getpid(), clients);
  fflush(stdout);
}

static void emit_ready_process(void) {
  printf("{\"type\":\"ready\",\"pid\":%d}\n", getpid());
  fflush(stdout);
}

static void emit_server_result(uint64_t requests, double elapsed_seconds) {
  printf(
    "{\"type\":\"result\",\"pid\":%d,\"requests\":%llu,\"elapsedSeconds\":%.6f}\n",
    getpid(),
    (unsigned long long)requests,
    elapsed_seconds
  );
  fflush(stdout);
}

static void emit_client_result(uint64_t sent, uint64_t received, uint64_t errors, double elapsed_seconds) {
  printf(
    "{\"type\":\"result\",\"pid\":%d,\"requestsSent\":%llu,\"responsesReceived\":%llu,\"errors\":%llu,\"elapsedSeconds\":%.6f}\n",
    getpid(),
    (unsigned long long)sent,
    (unsigned long long)received,
    (unsigned long long)errors,
    elapsed_seconds
  );
  fflush(stdout);
}

static ssize_t socket_send_no_sigpipe(int fd, const void *buffer, size_t length) {
#ifdef MSG_NOSIGNAL
  return send(fd, buffer, length, MSG_NOSIGNAL);
#else
  return send(fd, buffer, length, 0);
#endif
}

static int socket_write_all(int fd, const unsigned char *buffer, size_t length) {
  size_t offset = 0;

  while (offset < length) {
    ssize_t written = socket_send_no_sigpipe(fd, buffer + offset, length - offset);
    if (written < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (written == 0) return -1;
    offset += (size_t)written;
  }

  return 0;
}

static int raw_client_ensure_capacity(raw_burst_client_t *client, size_t needed) {
  unsigned char *resized;
  size_t next_capacity;

  if (!client) return -1;
  if (needed <= client->rx_capacity) return 0;

  next_capacity = client->rx_capacity ? client->rx_capacity : 65536;
  while (next_capacity < needed) next_capacity *= 2;

  resized = (unsigned char *)realloc(client->rx_buffer, next_capacity);
  if (!resized) return -1;

  client->rx_buffer = resized;
  client->rx_capacity = next_capacity;
  return 0;
}

static const unsigned char *find_bytes(
  const unsigned char *haystack,
  size_t haystack_length,
  const char *needle,
  size_t needle_length
) {
  size_t index;

  if (!haystack || !needle || needle_length == 0 || haystack_length < needle_length) return NULL;
  for (index = 0; index + needle_length <= haystack_length; index += 1) {
    if (memcmp(haystack + index, needle, needle_length) == 0) return haystack + index;
  }
  return NULL;
}

static const unsigned char *find_bytes_ci(
  const unsigned char *haystack,
  size_t haystack_length,
  const char *needle,
  size_t needle_length
) {
  size_t index;
  size_t offset;

  if (!haystack || !needle || needle_length == 0 || haystack_length < needle_length) return NULL;
  for (index = 0; index + needle_length <= haystack_length; index += 1) {
    for (offset = 0; offset < needle_length; offset += 1) {
      unsigned char a = haystack[index + offset];
      unsigned char b = (unsigned char)needle[offset];
      if (a >= 'A' && a <= 'Z') a = (unsigned char)(a - 'A' + 'a');
      if (b >= 'A' && b <= 'Z') b = (unsigned char)(b - 'A' + 'a');
      if (a != b) break;
    }
    if (offset == needle_length) return haystack + index;
  }
  return NULL;
}

static int connect_tcp_socket(const char *host, int port) {
  struct addrinfo hints;
  struct addrinfo *results = NULL;
  struct addrinfo *current;
  char port_text[32];
  int fd = -1;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  snprintf(port_text, sizeof(port_text), "%d", port);
  if (getaddrinfo(host, port_text, &hints, &results) != 0) return -1;

  for (current = results; current; current = current->ai_next) {
    int one = 1;

    fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd < 0) continue;

    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
    (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    if (connect(fd, current->ai_addr, current->ai_addrlen) == 0) break;

    close(fd);
    fd = -1;
  }

  freeaddrinfo(results);
  return fd;
}

static int raw_client_read_more(raw_burst_client_t *client) {
  ssize_t received;

  if (!client) return -1;
  if (raw_client_ensure_capacity(client, client->rx_length + 65536) != 0) return -1;

  received = recv(client->fd, client->rx_buffer + client->rx_length, client->rx_capacity - client->rx_length, 0);
  if (received < 0) {
    if (errno == EINTR) return raw_client_read_more(client);
    return -1;
  }
  if (received == 0) return -1;

  client->rx_length += (size_t)received;
  return 0;
}

static int raw_client_handshake(raw_burst_client_t *client, const char *host, int port) {
  unsigned char request[1024];
  int request_length;
  const unsigned char *header_end;
  size_t over_read;
  static const char websocket_key[] = "dGhlIHNhbXBsZSBub25jZQ==";

  request_length = snprintf(
    (char *)request,
    sizeof(request),
    "GET / HTTP/1.1\r\n"
    "content-length: 0\r\n"
    "upgrade: websocket\r\n"
    "sec-websocket-version: 13\r\n"
    "connection: upgrade\r\n"
    "sec-websocket-key: %s\r\n"
    "host: %s:%d\r\n"
    "\r\n",
    websocket_key,
    host,
    port
  );
  if (request_length <= 0 || (size_t)request_length >= sizeof(request)) return -1;
  if (socket_write_all(client->fd, request, (size_t)request_length) != 0) return -1;

  while (1) {
    header_end = find_bytes(client->rx_buffer, client->rx_length, "\r\n\r\n", 4);
    if (header_end) break;
    if (raw_client_read_more(client) != 0) return -1;
  }

  if (client->rx_length < 12 || memcmp(client->rx_buffer, "HTTP/1.1 101 ", 12) != 0) return -1;
  if (!find_bytes_ci(client->rx_buffer, (size_t)(header_end - client->rx_buffer), "sec-websocket-accept:", 21)) {
    return -1;
  }

  over_read = client->rx_length - ((size_t)(header_end - client->rx_buffer) + 4);
  if (over_read > 0) {
    memmove(client->rx_buffer, header_end + 4, over_read);
  }
  client->rx_length = over_read;
  return 0;
}

static int raw_client_connect(raw_burst_client_t *client, const char *host, int port) {
  if (!client || !host || port <= 0) return -1;

  memset(client, 0, sizeof(*client));
  client->fd = connect_tcp_socket(host, port);
  if (client->fd < 0) return -1;
  client->mask_seed = (uint32_t)now_ns();

  if (raw_client_ensure_capacity(client, 65536) != 0 || raw_client_handshake(client, host, port) != 0) {
    close(client->fd);
    free(client->rx_buffer);
    memset(client, 0, sizeof(*client));
    client->fd = -1;
    return -1;
  }

  return 0;
}

static void raw_client_close(raw_burst_client_t *client) {
  if (!client) return;
  if (client->fd >= 0) close(client->fd);
  free(client->rx_buffer);
  memset(client, 0, sizeof(*client));
  client->fd = -1;
}

static uint64_t count_occurrences(const unsigned char *haystack, size_t haystack_length, const char *needle) {
  size_t needle_length = strlen(needle);
  size_t index = 0;
  uint64_t count = 0;

  if (!haystack || !needle || needle_length == 0 || haystack_length < needle_length) return 0;

  while (index + needle_length <= haystack_length) {
    if (memcmp(haystack + index, needle, needle_length) == 0) {
      count += 1;
      index += needle_length;
    } else {
      index += 1;
    }
  }

  return count;
}

static int raw_client_send_frame(raw_burst_client_t *client, unsigned char opcode, const unsigned char *payload, size_t payload_length) {
  unsigned char header[14];
  unsigned char mask[4];
  unsigned char *frame;
  size_t header_length = 0;
  size_t index;

  if (!client || !payload) return -1;

  header[header_length++] = (unsigned char)(0x80u | (opcode & 0x0Fu));
  if (payload_length <= 125) {
    header[header_length++] = (unsigned char)(0x80u | payload_length);
  } else if (payload_length <= 65535) {
    header[header_length++] = 0x80u | 126u;
    header[header_length++] = (unsigned char)((payload_length >> 8) & 0xFFu);
    header[header_length++] = (unsigned char)(payload_length & 0xFFu);
  } else {
    header[header_length++] = 0x80u | 127u;
    for (index = 0; index < 8; index += 1) {
      header[header_length++] = (unsigned char)((payload_length >> ((7 - index) * 8)) & 0xFFu);
    }
  }

  client->mask_seed += 0x9e3779b9u;
  mask[0] = (unsigned char)((client->mask_seed >> 24) & 0xFFu);
  mask[1] = (unsigned char)((client->mask_seed >> 16) & 0xFFu);
  mask[2] = (unsigned char)((client->mask_seed >> 8) & 0xFFu);
  mask[3] = (unsigned char)(client->mask_seed & 0xFFu);
  memcpy(header + header_length, mask, sizeof(mask));
  header_length += sizeof(mask);

  frame = (unsigned char *)malloc(header_length + payload_length);
  if (!frame) return -1;

  memcpy(frame, header, header_length);
  for (index = 0; index < payload_length; index += 1) {
    frame[header_length + index] = (unsigned char)(payload[index] ^ mask[index & 3u]);
  }

  index = socket_write_all(client->fd, frame, header_length + payload_length);
  free(frame);
  return index;
}

static int append_request_text(char **buffer, size_t *length, size_t *capacity, uint64_t id) {
  int written;

  if (!buffer || !length || !capacity) return -1;

  if (*capacity - *length < 96) {
    size_t next_capacity = *capacity ? *capacity * 2 : 256;
    char *resized;
    while (next_capacity - *length < 96) next_capacity *= 2;
    resized = (char *)realloc(*buffer, next_capacity);
    if (!resized) return -1;
    *buffer = resized;
    *capacity = next_capacity;
  }

  written = snprintf(
    *buffer + *length,
    *capacity - *length,
    "{\"id\":%llu,\"path\":\"%s\",\"body\":{\"text\":\"%s\"}}",
    (unsigned long long)id,
    REQUEST_PATH,
    REQUEST_BODY_TEXT
  );
  if (written <= 0 || (size_t)written >= *capacity - *length) return -1;
  *length += (size_t)written;
  return 0;
}

static int raw_client_send_text_count_batch(raw_burst_client_t *client, uint64_t start_id, size_t count) {
  char *payload = NULL;
  size_t length = 0;
  size_t capacity = count > 1 ? (count * 96) + 2 : 96;
  size_t index;
  int rc;

  if (!client || count == 0) return -1;

  payload = (char *)malloc(capacity);
  if (!payload) return -1;

  if (count > 1) payload[length++] = '[';
  for (index = 0; index < count; index += 1) {
    if (index > 0) payload[length++] = ',';
    if (append_request_text(&payload, &length, &capacity, start_id + index) != 0) {
      free(payload);
      return -1;
    }
  }
  if (count > 1) payload[length++] = ']';

  rc = raw_client_send_frame(client, 0x1u, (const unsigned char *)payload, length);
  free(payload);
  return rc;
}

static void raw_client_consume(raw_burst_client_t *client, size_t length) {
  if (!client || length == 0 || length > client->rx_length) return;
  if (length < client->rx_length) memmove(client->rx_buffer, client->rx_buffer + length, client->rx_length - length);
  client->rx_length -= length;
}

static int raw_client_recv_counts(raw_burst_client_t *client, uint64_t *out_completed, uint64_t *out_errors) {
  if (out_completed) *out_completed = 0;
  if (out_errors) *out_errors = 0;
  if (!client) return -1;

  while (1) {
    unsigned char opcode;
    size_t offset;
    size_t payload_length;
    size_t frame_length;
    int masked;
    unsigned char *payload;
    size_t index;
    uint64_t completed;
    uint64_t errors;

    if (client->rx_length < 2) {
      if (raw_client_read_more(client) != 0) return -1;
      continue;
    }

    opcode = (unsigned char)(client->rx_buffer[0] & 0x0Fu);
    masked = (client->rx_buffer[1] & 0x80u) != 0;
    payload_length = (size_t)(client->rx_buffer[1] & 0x7Fu);
    offset = 2;

    if (payload_length == 126) {
      if (client->rx_length < offset + 2) {
        if (raw_client_read_more(client) != 0) return -1;
        continue;
      }
      payload_length = ((size_t)client->rx_buffer[offset] << 8) | (size_t)client->rx_buffer[offset + 1];
      offset += 2;
    } else if (payload_length == 127) {
      uint64_t parsed_length = 0;
      if (client->rx_length < offset + 8) {
        if (raw_client_read_more(client) != 0) return -1;
        continue;
      }
      for (index = 0; index < 8; index += 1) {
        parsed_length = (parsed_length << 8) | client->rx_buffer[offset + index];
      }
      payload_length = (size_t)parsed_length;
      offset += 8;
    }

    if (masked) offset += 4;
    frame_length = offset + payload_length;
    if (client->rx_length < frame_length) {
      if (raw_client_read_more(client) != 0) return -1;
      continue;
    }

    payload = client->rx_buffer + offset;
    if (masked) {
      unsigned char *mask = client->rx_buffer + (offset - 4);
      for (index = 0; index < payload_length; index += 1) {
        payload[index] = (unsigned char)(payload[index] ^ mask[index & 3u]);
      }
    }

    switch (opcode) {
      case 0x1u:
      case 0x2u:
        completed = count_occurrences(payload, payload_length, "\"id\":");
        errors = count_occurrences(payload, payload_length, "\"error\":");
        raw_client_consume(client, frame_length);
        if (out_completed) *out_completed = completed;
        if (out_errors) *out_errors = errors;
        return 0;

      case 0x9u:
        if (raw_client_send_frame(client, 0xAu, payload, payload_length) != 0) return -1;
        raw_client_consume(client, frame_length);
        break;

      case 0xAu:
        raw_client_consume(client, frame_length);
        break;

      case 0x8u:
        return -1;

      default:
        raw_client_consume(client, frame_length);
        break;
    }
  }
}

static int run_client_burst(const char *host, int port, int total_requests) {
  raw_burst_client_t client;
  uint64_t requests_sent = 0;
  uint64_t responses_received = 0;
  uint64_t errors = 0;
  uint64_t next_id = 1;
  uint64_t in_flight = 0;
  uint64_t in_flight_limit = (uint64_t)read_positive_env("BENCH_MAX_IN_FLIGHT", DEFAULT_MAX_IN_FLIGHT);
  int64_t started_ns;

  if (in_flight_limit == 0) in_flight_limit = 1;
  if (raw_client_connect(&client, host, port) != 0) {
    fprintf(stderr, "client connect failed\n");
    return 1;
  }

  emit_ready_client(1);
  started_ns = now_ns();

  while (requests_sent < (uint64_t)total_requests || in_flight > 0) {
    while (requests_sent < (uint64_t)total_requests && in_flight < in_flight_limit) {
      uint64_t remaining = (uint64_t)total_requests - requests_sent;
      uint64_t capacity = in_flight_limit - in_flight;
      uint64_t batch_count = remaining < capacity ? remaining : capacity;
      uint64_t completed;
      uint64_t batch_errors;

      if (batch_count > BURST_BATCH_SIZE) batch_count = BURST_BATCH_SIZE;
      if (raw_client_send_text_count_batch(&client, next_id, (size_t)batch_count) != 0) {
        errors += batch_count;
        requests_sent += batch_count;
        next_id += batch_count;
        continue;
      }

      requests_sent += batch_count;
      next_id += batch_count;
      in_flight += batch_count;

      (void)completed;
      (void)batch_errors;
    }

    if (in_flight > 0) {
      uint64_t completed = 0;
      uint64_t batch_errors = 0;
      if (raw_client_recv_counts(&client, &completed, &batch_errors) != 0) break;
      if (completed == 0) continue;
      if (batch_errors > completed) batch_errors = completed;
      if (completed > in_flight) completed = in_flight;
      in_flight -= completed;
      responses_received += completed - batch_errors;
      errors += batch_errors;
    }
  }

  emit_client_result(requests_sent, responses_received, errors, nanos_to_seconds(now_ns() - started_ns));
  raw_client_close(&client);
  return 0;
}

static int run_server(void) {
  channel_c_channel_t *channel;
  channel_c_server_t *server;
  char *error = NULL;
  int port = read_positive_env("BENCH_PORT", 2049);
  int64_t started_ns;

  install_signal_handlers();
  g_stop_requested = 0;
  g_server_requests = 0;

  channel = channel_c_channel_new();
  if (!channel) return 1;
  if (channel_c_channel_post(channel, REQUEST_PATH, count_text, NULL, 0) != 0) {
    channel_c_channel_free(channel);
    return 1;
  }

  server = channel_c_server_listen(channel, "127.0.0.1", port, (channel_c_state_factory_t){0}, &error);
  if (!server) {
    fprintf(stderr, "%s\n", error ? error : "server start failed");
    channel_c_free(error);
    channel_c_channel_free(channel);
    return 1;
  }

  emit_ready_server(channel_c_server_port(server));
  started_ns = now_ns();

  while (!g_stop_requested) {
    if (!channel_c_server_poll(server)) usleep(100);
  }

  emit_server_result(g_server_requests, nanos_to_seconds(now_ns() - started_ns));
  channel_c_server_free(server);
  channel_c_channel_free(channel);
  return 0;
}

static int run_client_serial(const char *host, int port, int clients_count, int total_requests) {
  channel_c_channel_t *channel;
  channel_c_client_t **clients = NULL;
  channel_c_sender_t **senders = NULL;
  char address[256];
  char *error = NULL;
  uint64_t requests_sent = 0;
  uint64_t responses_received = 0;
  uint64_t errors = 0;
  int64_t started_ns;
  int index;

  channel = channel_c_channel_new();
  if (!channel) return 1;

  clients = (channel_c_client_t **)calloc((size_t)clients_count, sizeof(channel_c_client_t *));
  senders = (channel_c_sender_t **)calloc((size_t)clients_count, sizeof(channel_c_sender_t *));
  if (!clients || !senders) {
    free(clients);
    free(senders);
    channel_c_channel_free(channel);
    return 1;
  }

  snprintf(address, sizeof(address), "ws://%s:%d", host, port);
  for (index = 0; index < clients_count; index += 1) {
    clients[index] = channel_c_client_connect(channel, address, NULL, NULL, &error);
    if (!clients[index]) {
      fprintf(stderr, "%s\n", error ? error : "client connect failed");
      channel_c_free(error);
      error = NULL;
      while (index > 0) {
        index -= 1;
        channel_c_client_free(clients[index]);
      }
      free(clients);
      free(senders);
      channel_c_channel_free(channel);
      return 1;
    }
    senders[index] = channel_c_client_sender(clients[index]);
  }

  emit_ready_client(clients_count);
  started_ns = now_ns();

  for (index = 0; index < total_requests; index += 1) {
    channel_c_sender_t *sender = senders[index % clients_count];
    char *body = NULL;
    if (channel_c_sender_send(sender, REQUEST_PATH, "{\"text\":\"hello\"}", NULL, &body, &error) == 0) {
      if (body && strcmp(body, "5") == 0) {
        responses_received += 1;
      } else {
        errors += 1;
      }
    } else {
      errors += 1;
    }
    requests_sent += 1;
    channel_c_free(body);
    channel_c_free(error);
    body = NULL;
    error = NULL;
  }

  emit_client_result(requests_sent, responses_received, errors, nanos_to_seconds(now_ns() - started_ns));

  for (index = 0; index < clients_count; index += 1) {
    channel_c_client_free(clients[index]);
  }
  free(clients);
  free(senders);
  channel_c_channel_free(channel);
  return 0;
}

static int run_client(void) {
  const char *host = getenv("BENCH_HOST");
  int clients_count = read_positive_env("BENCH_CLIENTS", 1);
  int total_requests = read_non_negative_env("BENCH_TOTAL_REQUESTS", 1);
  int port = read_positive_env("BENCH_PORT", 2049);
  request_mode_t request_mode = read_request_mode();

  if (!host || !host[0]) host = "127.0.0.1";
  if (request_mode == REQUEST_MODE_BURST && clients_count == 1) {
    return run_client_burst(host, port, total_requests);
  }
  return run_client_serial(host, port, clients_count, total_requests);
}

static int run_process(void) {
  channel_c_channel_t *channel;
  channel_c_process_t *process;
  channel_c_sender_t *sender;
  char *error = NULL;
  int total_requests = read_non_negative_env("BENCH_TOTAL_REQUESTS", 1);
  int64_t started_ns;
  int index;
  uint64_t sent = 0;
  uint64_t received = 0;
  uint64_t errors = 0;

  channel = channel_c_channel_new();
  if (!channel) return 1;
  if (channel_c_channel_post(channel, REQUEST_PATH, count_text_process, NULL, 0) != 0) {
    channel_c_channel_free(channel);
    return 1;
  }

  process = channel_c_process_new(channel, NULL, NULL);
  if (!process) {
    channel_c_channel_free(channel);
    return 1;
  }
  sender = channel_c_process_sender(process);
  emit_ready_process();
  started_ns = now_ns();

  for (index = 0; index < total_requests; index += 1) {
    char *body = NULL;
    if (channel_c_sender_send(sender, REQUEST_PATH, REQUEST_BODY_TEXT, NULL, &body, &error) == 0) {
      if (body && strcmp(body, "5") == 0) {
        received += 1;
      } else {
        errors += 1;
      }
    } else {
      errors += 1;
    }
    sent += 1;
    channel_c_free(body);
    channel_c_free(error);
    body = NULL;
    error = NULL;
  }

  emit_client_result(sent, received, errors, nanos_to_seconds(now_ns() - started_ns));
  channel_c_process_free(process);
  channel_c_channel_free(channel);
  return 0;
}

int main(int argc, char **argv) {
  const char *mode = argc > 1 ? argv[1] : "client";

  if (strcmp(mode, "server") == 0) return run_server();
  if (strcmp(mode, "client") == 0) return run_client();
  if (strcmp(mode, "process") == 0) return run_process();

  fprintf(stderr, "unknown mode: %s\n", mode);
  return 1;
}
