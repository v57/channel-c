#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <libwebsockets.h>

#include "internal.h"

typedef struct queued_message queued_message_t;
typedef struct server_client server_client_t;

struct queued_message {
  size_t length;
  int batch_safe;
  queued_message_t *next;
  unsigned char payload[];
};

struct server_client {
  struct channel_c_server *server;
  struct lws *wsi;
  channel_c_connection_t *connection;
  int closed;

  queued_message_t *queue_head;
  queued_message_t *queue_tail;

  char *rx_buffer;
  size_t rx_length;
  size_t rx_capacity;

  server_client_t *next;
};

struct channel_c_server {
  int running;
  unsigned long progress_counter;

  struct lws_context *context;
  channel_c_channel_t *channel;
  char *host;
  int requested_port;
  int actual_port;
  channel_c_state_factory_t state_factory;

  server_client_t *clients;
};

static int server_poll_local(void *ctx);
static int server_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length);

static struct lws_protocols server_protocols[] = {
  {
    .name = "channel-c",
    .callback = server_ws_callback,
    .per_session_data_size = sizeof(server_client_t *),
    .rx_buffer_size = 0,
    .id = 0,
    .user = NULL,
    .tx_packet_size = 0,
  },
  { NULL, NULL, 0, 0, 0, NULL, 0 }
};

static void queued_message_free_all(queued_message_t *message) {
  while (message) {
    queued_message_t *next = message->next;
    free(message);
    message = next;
  }
}

static int server_queue_push(server_client_t *client, const char *text) {
  queued_message_t *message;
  size_t length;

  if (!client || !text || client->closed) return -1;
  length = strlen(text);
  if (channel_c_internal_failpoint_hit("server.queue_alloc_fail")) return -1;

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

static queued_message_t *server_queue_pop_batch(
  server_client_t *client,
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

static int server_client_send_text(void *ctx, const char *text) {
  server_client_t *client = (server_client_t *)ctx;
  if (!client || !text || client->closed) return -1;
  if (server_queue_push(client, text) != 0) return -1;
  if (client->wsi) lws_callback_on_writable(client->wsi);
  if (client->server && client->server->context) lws_cancel_service(client->server->context);
  return 0;
}

static void server_client_close_transport(void *ctx) {
  server_client_t *client = (server_client_t *)ctx;
  if (!client) return;
  client->closed = 1;
  if (client->wsi) lws_set_timeout(client->wsi, PENDING_TIMEOUT_CLOSE_SEND, 1);
  if (client->server && client->server->context) lws_cancel_service(client->server->context);
}

static void server_remove_client(struct channel_c_server *server, server_client_t *client) {
  server_client_t *previous = NULL;
  server_client_t *current;

  if (!server || !client) return;
  current = server->clients;
  while (current) {
    if (current == client) {
      if (previous) {
        previous->next = current->next;
      } else {
        server->clients = current->next;
      }
      return;
    }
    previous = current;
    current = current->next;
  }
}

static int server_rx_append(server_client_t *client, const char *data, size_t length, size_t remaining) {
  char *buffer;
  size_t required;

  if (!client || !data || length == 0) return 0;
  /* Cap whole messages (fragments included) instead of buffering without bound. `remaining` is the
   * rest of the current frame as announced by its header, so oversized frames fail early. */
  if (length > CHANNEL_C_MAX_MESSAGE_SIZE - client->rx_length ||
      remaining > CHANNEL_C_MAX_MESSAGE_SIZE - client->rx_length - length) {
    return -1;
  }
  required = client->rx_length + length + 1;
  if (required > client->rx_capacity) {
    size_t capacity = client->rx_capacity ? client->rx_capacity : 1024;
    while (capacity < required) capacity *= 2;
    if (channel_c_internal_failpoint_hit("server.rx_realloc_fail")) return -1;
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

typedef struct {
  channel_c_header_t *items;
  size_t count;
  size_t capacity;
  struct lws *wsi;
  int failed;
} header_list_t;

static void header_list_free(header_list_t *list) {
  size_t index;
  for (index = 0; index < list->count; index += 1) {
    free((char *)list->items[index].name);
    free((char *)list->items[index].value);
  }
  free(list->items);
  list->items = NULL;
  list->count = 0;
  list->capacity = 0;
}

/* Sets name (lowercased, trailing ':' dropped) = value; a later set for the same name wins. */
static void header_list_set(header_list_t *list, const char *name, size_t name_length, const char *value, size_t value_length) {
  char *owned_name;
  char *owned_value;
  size_t index;

  while (name_length > 0 && (name[name_length - 1] == ':' || name[name_length - 1] == ' ')) name_length -= 1;
  if (name_length == 0 || list->failed) return;

  owned_name = (char *)malloc(name_length + 1);
  owned_value = (char *)malloc(value_length + 1);
  if (!owned_name || !owned_value) {
    free(owned_name);
    free(owned_value);
    list->failed = 1;
    return;
  }
  for (index = 0; index < name_length; index += 1) owned_name[index] = (char)tolower((unsigned char)name[index]);
  owned_name[name_length] = '\0';
  memcpy(owned_value, value, value_length);
  owned_value[value_length] = '\0';

  for (index = 0; index < list->count; index += 1) {
    if (strcmp(list->items[index].name, owned_name) == 0) {
      free(owned_name);
      free((char *)list->items[index].value);
      list->items[index].value = owned_value;
      return;
    }
  }
  if (list->count == list->capacity) {
    size_t capacity = list->capacity ? list->capacity * 2 : 16;
    channel_c_header_t *items = (channel_c_header_t *)realloc(list->items, capacity * sizeof(channel_c_header_t));
    if (!items) {
      free(owned_name);
      free(owned_value);
      list->failed = 1;
      return;
    }
    list->items = items;
    list->capacity = capacity;
  }
  list->items[list->count].name = owned_name;
  list->items[list->count].value = owned_value;
  list->count += 1;
}

static void header_list_add_custom(const char *name, int name_length, void *opaque) {
  header_list_t *list = (header_list_t *)opaque;
  int value_length;
  char *value;

  if (!list || !name || name_length <= 0) return;
  value_length = lws_hdr_custom_length(list->wsi, name, name_length);
  if (value_length < 0) return;
  value = (char *)malloc((size_t)value_length + 1);
  if (!value) {
    list->failed = 1;
    return;
  }
  if (lws_hdr_custom_copy(list->wsi, value, value_length + 1, name, name_length) >= 0) {
    header_list_set(list, name, (size_t)name_length, value, strlen(value));
  }
  free(value);
}

/* Query parameters first, then request headers, so headers win (TS server.ts). */
static void server_collect_headers(struct lws *wsi, header_list_t *list) {
  char fragment[2048];
  int index;
  int token;

  list->wsi = wsi;
  for (index = 0; index < 256; index += 1) {
    int fragment_length = lws_hdr_copy_fragment(wsi, fragment, (int)sizeof(fragment), WSI_TOKEN_HTTP_URI_ARGS, index);
    const char *separator;
    if (fragment_length < 0) break;
    separator = memchr(fragment, '=', (size_t)fragment_length);
    if (separator) {
      header_list_set(list, fragment, (size_t)(separator - fragment), separator + 1,
                      (size_t)fragment_length - (size_t)(separator - fragment) - 1);
    } else {
      header_list_set(list, fragment, (size_t)fragment_length, "", 0);
    }
  }

  for (token = 0; token < (int)WSI_TOKEN_COUNT; token += 1) {
    const unsigned char *name = lws_token_to_string((enum lws_token_indexes)token);
    size_t name_length;
    int value_length;
    char *value;

    if (!name) continue;
    name_length = strlen((const char *)name);
    /* Only real "name:" headers; skip pseudo tokens like "get " / ":path". */
    if (name_length < 2 || name[0] == ':' || name[name_length - 1] != ':') continue;
    value_length = lws_hdr_total_length(wsi, (enum lws_token_indexes)token);
    if (value_length <= 0) continue;
    value = (char *)malloc((size_t)value_length + 1);
    if (!value) {
      list->failed = 1;
      return;
    }
    if (lws_hdr_copy(wsi, value, value_length + 1, (enum lws_token_indexes)token) >= 0) {
      header_list_set(list, (const char *)name, name_length, value, strlen(value));
    }
    free(value);
  }

  (void)lws_hdr_custom_name_foreach(wsi, header_list_add_custom, list);
}

static void server_client_disconnect_hook(struct channel_c_server *server, server_client_t *client) {
  if (!server || !client || !client->connection || !server->state_factory.on_disconnect) return;
  server->state_factory.on_disconnect(
    channel_c_connection_state(client->connection),
    channel_c_connection_sender(client->connection),
    server->state_factory.user_data
  );
}

static int server_ws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t length) {
  struct channel_c_server *server = (struct channel_c_server *)lws_context_user(lws_get_context(wsi));
  server_client_t **client_slot = (server_client_t **)user;
  server_client_t *client = client_slot ? *client_slot : NULL;

  if (!server) return 0;

  switch (reason) {
    case LWS_CALLBACK_ESTABLISHED:
      {
        channel_c_transport_ops_t transport_ops;
        void *state = NULL;

        server->progress_counter += 1;
        if (channel_c_internal_failpoint_hit("server.client_alloc_fail")) return -1;
        client = (server_client_t *)calloc(1, sizeof(server_client_t));
        if (!client) return -1;

        if (server->state_factory.create_with_headers) {
          header_list_t list;
          channel_c_headers_t headers;
          memset(&list, 0, sizeof(list));
          server_collect_headers(wsi, &list);
          if (list.failed) {
            header_list_free(&list);
            free(client);
            return -1;
          }
          headers.items = list.items;
          headers.count = list.count;
          state = server->state_factory.create_with_headers(&headers, server->state_factory.user_data);
          header_list_free(&list);
        } else if (server->state_factory.create) {
          state = server->state_factory.create(server->state_factory.user_data);
        }

        transport_ops.send_text = server_client_send_text;
        transport_ops.close = server_client_close_transport;

        client->server = server;
        client->wsi = wsi;
        client->closed = 0;
        client->connection = channel_c_internal_failpoint_hit("server.connection_create_fail")
                               ? NULL
                               : channel_c_connection_create(
                                   server->channel,
                                   state,
                                   server->state_factory.destroy,
                                   server->state_factory.user_data,
                                   client,
                                   CHANNEL_C_TRANSPORT_REMOTE,
                                   transport_ops
                                 );
        if (!client->connection) {
          if (server->state_factory.destroy && state) {
            server->state_factory.destroy(state, server->state_factory.user_data);
          }
          free(client);
          return -1;
        }

        client->next = server->clients;
        server->clients = client;
        if (client_slot) *client_slot = client;
        channel_c_connection_mark_connected(client->connection, 1);
        if (server->state_factory.on_connect) {
          server->state_factory.on_connect(
            channel_c_connection_state(client->connection),
            channel_c_connection_sender(client->connection),
            server->state_factory.user_data
          );
        }
      }
      break;

    case LWS_CALLBACK_RECEIVE:
      if (!client) return -1;
      if (server_rx_append(client, (const char *)in, length, lws_remaining_packet_payload(wsi)) != 0) return -1;
      if (lws_is_final_fragment(wsi) && lws_remaining_packet_payload(wsi) == 0) {
        server->progress_counter += 1;
        channel_c_connection_receive_text(client->connection, client->rx_buffer);
        client->rx_length = 0;
      }
      break;

    case LWS_CALLBACK_SERVER_WRITEABLE:
      if (client) {
        queued_message_t *batch;
        size_t batch_count = 0;
        size_t payload_bytes = 0;
        int write_failed = 0;

        batch = server_queue_pop_batch(client, 256, 256 * 1024, &batch_count, &payload_bytes);
        if (batch) {
          if (batch_count == 1) {
            if (channel_c_internal_failpoint_hit("server.write_fail") ||
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
              if (channel_c_internal_failpoint_hit("server.write_fail") ||
                  lws_write(wsi, buffer + LWS_PRE, offset, LWS_WRITE_TEXT) < 0) {
                write_failed = 1;
              }
              free(buffer);
            }
          }

          queued_message_free_all(batch);
          server->progress_counter += 1;
        }

        if (write_failed) return -1;
        if (client->queue_head) lws_callback_on_writable(wsi);
      }
      break;

    case LWS_CALLBACK_CLOSED:
      server->progress_counter += 1;
      if (client) {
        client->closed = 1;
        client->wsi = NULL;
        server_remove_client(server, client);
        channel_c_connection_on_close(client->connection);
        server_client_disconnect_hook(server, client);
        channel_c_connection_release(client->connection);
        queued_message_free_all(client->queue_head);
        free(client->rx_buffer);
        free(client);
        if (client_slot) *client_slot = NULL;
      }
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      break;

    default:
      break;
  }

  return 0;
}

static int server_poll_local(void *ctx) {
  struct channel_c_server *server = (struct channel_c_server *)ctx;
  server_client_t *client;
  unsigned long start_progress;
  unsigned long loop_progress;
  int iterations = 0;

  if (!server || !server->context || !server->running) return 0;

  start_progress = server->progress_counter;
  do {
    loop_progress = server->progress_counter;
    client = server->clients;
    while (client) {
      if (client->wsi && client->queue_head) lws_callback_on_writable(client->wsi);
      client = client->next;
    }

    lws_cancel_service(server->context);
    lws_service(server->context, 0);
    iterations += 1;
  } while (iterations < 32 && server->progress_counter != loop_progress);

  return server->progress_counter != start_progress ? 1 : 0;
}

channel_c_server_t *channel_c_server_listen(
  channel_c_channel_t *channel,
  const char *host,
  int port,
  channel_c_state_factory_t state_factory,
  char **out_error
) {
  struct channel_c_server *server;
  struct lws_context_creation_info info;
  struct lws_vhost *vhost;

  if (out_error) *out_error = NULL;
  if (!channel || port < 0) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return NULL;
  }

  if (channel_c_internal_failpoint_hit("server.server_alloc_fail")) {
    server = NULL;
  } else {
    server = (struct channel_c_server *)calloc(1, sizeof(struct channel_c_server));
  }
  if (!server) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  server->channel = channel;
  server->requested_port = port;
  server->host = channel_c_internal_failpoint_hit("server.host_strdup_fail")
                   ? NULL
                   : channel_c_strdup(host && host[0] ? host : "127.0.0.1");
  server->state_factory = state_factory;
  server->running = 1;
  if (!server->host) {
    free(server);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  memset(&info, 0, sizeof(info));
  info.port = port;
  info.iface = server->host;
  info.protocols = server_protocols;
  info.gid = -1;
  info.uid = -1;
  info.user = server;

  server->context = channel_c_internal_failpoint_hit("server.context_create_fail")
                      ? NULL
                      : lws_create_context(&info);
  if (!server->context) {
    free(server->host);
    free(server);
    if (out_error) *out_error = channel_c_strdup("failed to create websocket server context");
    return NULL;
  }

  vhost = lws_get_vhost_by_name(server->context, "default");
  server->actual_port = vhost ? lws_get_vhost_listen_port(vhost) : port;
  if (server->actual_port < 0) server->actual_port = port;

  channel_c_internal_register_pump(server, server_poll_local);
  return server;
}

int channel_c_server_poll(channel_c_server_t *server) {
  return server_poll_local((void *)server);
}

int channel_c_server_port(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  if (!s) return 0;
  return s->actual_port;
}

void channel_c_server_stop(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  struct lws_context *context;

  if (!s) return;
  s->running = 0;
  channel_c_internal_unregister_pump(s);

  context = s->context;
  s->context = NULL;
  if (context) lws_context_destroy(context);

  while (s->clients) {
    server_client_t *client = s->clients;
    s->clients = client->next;
    client->closed = 1;
    client->wsi = NULL;
    channel_c_connection_on_close(client->connection);
    server_client_disconnect_hook(s, client);
    channel_c_connection_release(client->connection);
    queued_message_free_all(client->queue_head);
    free(client->rx_buffer);
    free(client);
  }
}

void channel_c_server_free(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  if (!s) return;
  channel_c_server_stop(server);
  free(s->host);
  free(s);
}

int channel_c_server_connection_count(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  server_client_t *client;
  int count = 0;

  if (!s) return 0;
  client = s->clients;
  while (client) {
    count += 1;
    client = client->next;
  }
  return count;
}

channel_c_connection_t *channel_c_internal_server_first_connection(channel_c_server_t *server) {
  struct channel_c_server *s = (struct channel_c_server *)server;
  if (!s || !s->clients) return NULL;
  return s->clients->connection;
}
