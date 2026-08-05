#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libwebsockets.h>

#include "internal.h"

typedef struct queued_message queued_message_t;

struct queued_message {
  size_t length;
  int batch_safe;
  queued_message_t *next;
  unsigned char payload[];
};

struct channel_c_client {
  int running;
  int connected;
  int connect_failed;
  unsigned long progress_counter;

  struct lws_context *context;
  struct lws *wsi;

  char *url;
  char *protocol;
  char *address;
  char *path;
  int port;
  int ssl;
  char *last_error;

  queued_message_t *queue_head;
  queued_message_t *queue_tail;

  char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;

  channel_c_connection_t *connection;
};

static int client_poll_local(void *ctx);
static int client_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length);

static struct lws_protocols client_protocols[] = {
  {
    .name = "channel-c",
    .callback = client_ws_callback,
    /* libwebsockets needs non-zero client per-session storage here for reliable handshake progress. */
    .per_session_data_size = sizeof(void *),
    .rx_buffer_size = 0,
    .id = 0,
    .user = NULL,
    .tx_packet_size = 0,
  },
  { NULL, NULL, 0, 0, 0, NULL, 0 }
};

static int64_t client_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64_t)ts.tv_sec * 1000LL) + ((int64_t)ts.tv_nsec / 1000000LL);
}

static int client_wait_connected(struct channel_c_client *client, int timeout_ms) {
  int64_t deadline = client_now_ms() + timeout_ms;

  while (client && !client->connected && !client->connect_failed && !channel_c_connection_is_closed(client->connection)) {
    if (!channel_c_internal_pump_all()) channel_c_internal_sleep_short();
    if (client_now_ms() > deadline) return -1;
  }

  return client && channel_c_connection_is_connected(client->connection) ? 0 : -1;
}

static void queued_message_free_all(queued_message_t *message) {
  while (message) {
    queued_message_t *next = message->next;
    free(message);
    message = next;
  }
}

static int client_queue_push(struct channel_c_client *client, const char *text) {
  queued_message_t *message;
  size_t length;

  if (!client || !text) return -1;
  length = strlen(text);
  if (channel_c_internal_failpoint_hit("client.queue_alloc_fail")) return -1;

  message = (queued_message_t *)calloc(1, sizeof(queued_message_t) + LWS_PRE + length);
  if (!message) return -1;

  message->length = length;
  message->batch_safe = text[0] == '{';
  memcpy(message->payload + LWS_PRE, text, length);

  if (client->queue_tail) {
    client->queue_tail->next = message;
  } else {
    client->queue_head = message;
  }
  client->queue_tail = message;
  return 0;
}

static queued_message_t *client_queue_pop_batch(
  struct channel_c_client *client,
  size_t max_messages,
  size_t max_payload_bytes,
  size_t *out_count,
  size_t *out_payload_bytes
) {
  queued_message_t *head = NULL;
  queued_message_t *tail = NULL;
  size_t count = 0;
  size_t payload_bytes = 0;
  int batch_safe = 1;

  if (out_count) *out_count = 0;
  if (out_payload_bytes) *out_payload_bytes = 0;
  if (!client) return NULL;

  while (count < max_messages && client->queue_head) {
    queued_message_t *message = client->queue_head;
    size_t candidate_bytes;

    if (count > 0 && (!batch_safe || !message->batch_safe)) break;

    candidate_bytes = payload_bytes + message->length + (count > 0 ? 1 : 0);
    if (count > 0 && candidate_bytes + 2 > max_payload_bytes) break;

    client->queue_head = message->next;
    if (!client->queue_head) client->queue_tail = NULL;
    message->next = NULL;

    if (tail) {
      tail->next = message;
    } else {
      head = message;
    }
    tail = message;

    batch_safe = batch_safe && message->batch_safe;
    payload_bytes = candidate_bytes;
    count += 1;
  }

  if (count == 0) return NULL;
  if (count > 1) payload_bytes += 2;

  if (out_count) *out_count = count;
  if (out_payload_bytes) *out_payload_bytes = payload_bytes;
  return head;
}

static int client_transport_send_text(void *ctx, const char *text) {
  struct channel_c_client *client = (struct channel_c_client *)ctx;

  if (!client || !text || !client->running) return -1;
  if (client_queue_push(client, text) != 0) return -1;

  if (client->context) lws_cancel_service(client->context);
  if (client->wsi) lws_callback_on_writable(client->wsi);
  return 0;
}

static void client_transport_close(void *ctx) {
  struct channel_c_client *client = (struct channel_c_client *)ctx;
  if (!client) return;
  client->running = 0;
  if (client->wsi) {
    lws_set_timeout(client->wsi, PENDING_TIMEOUT_CLOSE_SEND, 1);
  }
  if (client->context) lws_cancel_service(client->context);
}

static int client_rx_append(struct channel_c_client *client, const char *data, size_t length) {
  char *buffer;
  size_t required;

  if (!client || !data || length == 0) return 0;

  required = client->rx_length + length + 1;
  if (required > client->rx_capacity) {
    size_t capacity = client->rx_capacity ? client->rx_capacity : 1024;
    while (capacity < required) capacity *= 2;
    if (channel_c_internal_failpoint_hit("client.rx_realloc_fail")) return -1;
    buffer = (char *)realloc(client->rx_buffer, capacity);
    if (!buffer) return -1;
    client->rx_buffer = buffer;
    client->rx_capacity = capacity;
  }

  memcpy(client->rx_buffer + client->rx_length, data, length);
  client->rx_length += length;
  client->rx_buffer[client->rx_length] = '\0';
  return 0;
}

static void client_mark_failed(struct channel_c_client *client, const char *error_text) {
  if (!client) return;
  client->connect_failed = 1;
  client->connected = 0;
  free(client->last_error);
  client->last_error = channel_c_strdup(error_text ? error_text : "connection error");
  channel_c_connection_on_close(client->connection);
}

static int client_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length) {
  struct lws_context *context = lws_get_context(wsi);
  struct channel_c_client *client = (struct channel_c_client *)lws_context_user(context);
  (void)user;

  if (!client) return 0;

  switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
      client->wsi = wsi;
      client->connected = 1;
      client->connect_failed = 0;
      client->progress_counter += 1;
      channel_c_connection_mark_connected(client->connection, 1);
      if (client->queue_head) lws_callback_on_writable(wsi);
      break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
      if (client_rx_append(client, (const char *)in, length) != 0) return -1;
      if (lws_is_final_fragment(wsi) && lws_remaining_packet_payload(wsi) == 0) {
        client->progress_counter += 1;
        channel_c_connection_receive_text(client->connection, client->rx_buffer);
        client->rx_length = 0;
      }
      break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
      {
        queued_message_t *batch;
        size_t batch_count = 0;
        size_t payload_bytes = 0;
        int write_failed = 0;

        batch = client_queue_pop_batch(client, 256, 256 * 1024, &batch_count, &payload_bytes);
        if (batch) {
          if (batch_count == 1) {
            if (channel_c_internal_failpoint_hit("client.write_fail") ||
                lws_write(wsi, batch->payload + LWS_PRE, batch->length, LWS_WRITE_TEXT) < 0) {
              write_failed = 1;
            }
          } else {
            unsigned char *buffer = (unsigned char *)malloc(LWS_PRE + payload_bytes);
            queued_message_t *current = batch;
            size_t offset = 0;
            if (!buffer) {
              write_failed = 1;
            } else {
              buffer[LWS_PRE + offset++] = '[';
              while (current) {
                memcpy(buffer + LWS_PRE + offset, current->payload + LWS_PRE, current->length);
                offset += current->length;
                if (current->next) buffer[LWS_PRE + offset++] = ',';
                current = current->next;
              }
              buffer[LWS_PRE + offset++] = ']';
              if (channel_c_internal_failpoint_hit("client.write_fail") ||
                  lws_write(wsi, buffer + LWS_PRE, offset, LWS_WRITE_TEXT) < 0) {
                write_failed = 1;
              }
              free(buffer);
            }
          }

          queued_message_free_all(batch);
          client->progress_counter += 1;
        }

        if (write_failed) return -1;
        if (client->queue_head) lws_callback_on_writable(wsi);
      }
      break;

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
      client->progress_counter += 1;
      client->wsi = NULL;
      client_mark_failed(client, in ? (const char *)in : "connection error");
      break;

    case LWS_CALLBACK_CLIENT_CLOSED:
      client->progress_counter += 1;
      client->connected = 0;
      client->wsi = NULL;
      channel_c_connection_on_close(client->connection);
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      if (client->wsi && client->queue_head) lws_callback_on_writable(client->wsi);
      break;

    default:
      break;
  }

  return 0;
}

static int parse_url(
  const char *url,
  char **out_protocol,
  char **out_address,
  int *out_port,
  char **out_path,
  int *out_ssl
) {
  char *copy;
  const char *protocol = NULL;
  const char *address = NULL;
  const char *path = NULL;
  int port = 0;

  if (!url || !out_protocol || !out_address || !out_port || !out_path || !out_ssl) return -1;

  copy = channel_c_strdup(url);
  if (!copy) return -1;

  if (channel_c_internal_failpoint_hit("client.parse_url_fail") || lws_parse_uri(copy, &protocol, &address, &port, &path) != 0) {
    free(copy);
    return -1;
  }

  *out_protocol = channel_c_strdup(protocol ? protocol : "ws");
  *out_address = channel_c_strdup(address ? address : "127.0.0.1");
  *out_path = channel_c_strdup(path ? path : "/");
  *out_port = port > 0 ? port : 80;
  *out_ssl = protocol && strcmp(protocol, "wss") == 0 ? 1 : 0;

  free(copy);

  if (!*out_protocol || !*out_address || !*out_path) {
    free(*out_protocol);
    free(*out_address);
    free(*out_path);
    *out_protocol = NULL;
    *out_address = NULL;
    *out_path = NULL;
    return -1;
  }

  return 0;
}

static int client_poll_local(void *ctx) {
  struct channel_c_client *client = (struct channel_c_client *)ctx;
  unsigned long start_progress;
  unsigned long loop_progress;
  int iterations = 0;

  if (!client || !client->context || !client->running) return 0;

  start_progress = client->progress_counter;
  do {
    loop_progress = client->progress_counter;
    if (client->wsi && client->queue_head) lws_callback_on_writable(client->wsi);
    lws_cancel_service(client->context);
    lws_service(client->context, 0);
    iterations += 1;
  } while (iterations < 32 && client->progress_counter != loop_progress);

  return client->progress_counter != start_progress ? 1 : 0;
}

channel_c_client_t *channel_c_client_connect(
  channel_c_channel_t *channel,
  const char *url,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  char **out_error
) {
  struct channel_c_client *client;
  struct lws_context_creation_info context_info;
  struct lws_client_connect_info connect_info;
  channel_c_transport_ops_t transport_ops;

  if (out_error) *out_error = NULL;
  if (!channel || !url) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("client.client_alloc_fail")) {
    client = NULL;
  } else {
    client = (struct channel_c_client *)calloc(1, sizeof(struct channel_c_client));
  }
  if (!client) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  client->running = 1;
  client->url = channel_c_strdup(url);
  if (!client->url) {
    free(client);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  if (parse_url(url, &client->protocol, &client->address, &client->port, &client->path, &client->ssl) != 0) {
    free(client->url);
    free(client);
    if (out_error) *out_error = channel_c_strdup("invalid websocket url");
    return NULL;
  }

  transport_ops.send_text = client_transport_send_text;
  transport_ops.close = client_transport_close;
  client->connection = channel_c_internal_failpoint_hit("client.connection_create_fail")
                         ? NULL
                         : channel_c_connection_create(
                             channel,
                             state,
                             state_destroy,
                             NULL,
                             client,
                             CHANNEL_C_TRANSPORT_REMOTE,
                             transport_ops
                           );
  if (!client->connection) {
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  memset(&context_info, 0, sizeof(context_info));
  context_info.port = CONTEXT_PORT_NO_LISTEN;
  context_info.protocols = client_protocols;
  context_info.gid = -1;
  context_info.uid = -1;
  context_info.user = client;

  client->context = channel_c_internal_failpoint_hit("client.context_create_fail")
                      ? NULL
                      : lws_create_context(&context_info);
  if (!client->context) {
    channel_c_connection_stop(client->connection);
    channel_c_connection_release(client->connection);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("failed to create websocket context");
    return NULL;
  }

  memset(&connect_info, 0, sizeof(connect_info));
  connect_info.context = client->context;
  connect_info.address = client->address;
  connect_info.port = client->port;
  connect_info.path = client->path;
  connect_info.host = client->address;
  connect_info.origin = client->address;
  connect_info.ssl_connection = client->ssl ? LCCSCF_USE_SSL : 0;
  connect_info.protocol = client_protocols[0].name;

  channel_c_internal_register_pump(client, client_poll_local);

  client->wsi = channel_c_internal_failpoint_hit("client.connect_via_info_fail")
                  ? NULL
                  : lws_client_connect_via_info(&connect_info);
  if (!client->wsi) {
    channel_c_internal_unregister_pump(client);
    lws_context_destroy(client->context);
    client->context = NULL;
    channel_c_connection_stop(client->connection);
    channel_c_connection_release(client->connection);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client);
    if (out_error) *out_error = channel_c_strdup("failed to connect websocket");
    return NULL;
  }

  if (client_wait_connected(client, 10000) != 0) {
    char *error_text = client->last_error ? channel_c_strdup(client->last_error) : channel_c_strdup("failed to connect websocket");
    channel_c_internal_unregister_pump(client);
    lws_context_destroy(client->context);
    client->context = NULL;
    channel_c_connection_stop(client->connection);
    channel_c_connection_release(client->connection);
    free(client->rx_buffer);
    free(client->url);
    free(client->protocol);
    free(client->address);
    free(client->path);
    free(client->last_error);
    queued_message_free_all(client->queue_head);
    free(client);
    if (out_error) *out_error = error_text;
    else free(error_text);
    return NULL;
  }

  return client;
}

channel_c_sender_t *channel_c_client_sender(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return NULL;
  return channel_c_connection_sender(c->connection);
}

int channel_c_client_send_string(
  channel_c_client_t *client,
  const char *path,
  const char *body_text,
  char **out_body_text,
  char **out_error
) {
  channel_c_sender_t *sender = channel_c_client_sender(client);
  if (!sender) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    if (out_body_text) *out_body_text = NULL;
    return -1;
  }
  return channel_c_sender_send_string(sender, path, body_text, out_body_text, out_error);
}

int channel_c_client_poll(channel_c_client_t *client) {
  return client_poll_local((void *)client);
}

int channel_c_client_is_connected(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return 0;
  return c->connected && channel_c_connection_is_connected(c->connection);
}

int channel_c_client_remote_is_subscribed(channel_c_client_t *client, const char *topic) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return 0;
  return channel_c_connection_is_remote_subscribed(c->connection, topic);
}

void channel_c_client_stop(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  struct lws_context *context;

  if (!c) return;
  c->running = 0;
  channel_c_internal_unregister_pump(c);

  context = c->context;
  c->context = NULL;
  c->wsi = NULL;
  c->connected = 0;
  if (context) lws_context_destroy(context);

  channel_c_connection_stop(c->connection);
}

void channel_c_client_free(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  if (!c) return;

  channel_c_client_stop(client);
  queued_message_free_all(c->queue_head);
  free(c->rx_buffer);
  free(c->url);
  free(c->protocol);
  free(c->address);
  free(c->path);
  free(c->last_error);
  channel_c_connection_release(c->connection);
  free(c);
}

channel_c_connection_t *channel_c_internal_client_connection(channel_c_client_t *client) {
  struct channel_c_client *c = (struct channel_c_client *)client;
  return c ? c->connection : NULL;
}
