#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <yyjson.h>

#include "internal.h"

typedef struct post_entry post_entry_t;
typedef struct stream_entry stream_entry_t;
typedef struct subscription_entry subscription_entry_t;
typedef struct pending_request pending_request_t;
typedef struct pending_stream_item pending_stream_item_t;
typedef struct listener_entry listener_entry_t;
typedef struct listener_callback listener_callback_t;
typedef struct topic_interest topic_interest_t;
typedef struct active_stream active_stream_t;

typedef enum {
  PENDING_CALL = 0,
  PENDING_STREAM = 1,
  PENDING_SUBSCRIPTION = 2,
} pending_kind_t;

typedef struct {
  char *name;
  int remaining;
} failpoint_entry_t;

typedef struct {
  void *ctx;
  channel_c_poll_fn poll_fn;
} pump_entry_t;

struct post_entry {
  char *path;
  size_t path_length;
  channel_c_post_handler_fn handler;
  void *user_data;
  unsigned int flags;
  post_entry_t *next;
};

struct stream_entry {
  char *path;
  channel_c_stream_handler_fn handler;
  void *user_data;
  stream_entry_t *next;
};

struct subscription_entry {
  char *name;
  char *prefix;
  channel_c_subscription_resolver_fn resolver;
  void *user_data;
  subscription_entry_t *next;
};

struct pending_stream_item {
  char *body_json;
  char *error_text;
  int done;
  pending_stream_item_t *next;
};

struct pending_request {
  int id;
  pending_kind_t kind;
  int completed;
  char *body_json;
  char *error_text;
  char *topic;
  int stream_finished;
  pending_stream_item_t *stream_head;
  pending_stream_item_t *stream_tail;
  pending_request_t *next;
};

struct listener_callback {
  int id;
  channel_c_event_callback_fn callback;
  void *user_data;
  listener_callback_t *next;
};

struct listener_entry {
  char *topic;
  listener_callback_t *callbacks;
  listener_entry_t *next;
};

struct topic_interest {
  char *topic;
  topic_interest_t *next;
};

struct active_stream {
  int id;
  int cancelled;
  active_stream_t *next;
};

struct channel_c_sender {
  channel_c_connection_t *connection;
};

struct channel_c_values {
  channel_c_connection_t *connection;
  int request_id;
  int closed;
};

struct channel_c_subscription_handle {
  channel_c_connection_t *connection;
  char *topic;
  int callback_id;
  int cancelled;
};

struct channel_c_stream_emitter {
  channel_c_connection_t *connection;
  int request_id;
  active_stream_t *active;
};

struct channel_c_channel {
  post_entry_t *posts;
  stream_entry_t *streams;
  subscription_entry_t *subscriptions;
  channel_c_connection_t *connections;
};

struct channel_c_connection {
  int ref_count;
  int connected;
  int closed;
  int next_request_id;
  int next_callback_id;

  channel_c_channel_t *channel;
  channel_c_connection_t *channel_next;

  pending_request_t *pending;
  listener_entry_t *listeners;
  topic_interest_t *remote_topics;
  active_stream_t *active_streams;

  channel_c_sender_t sender;

  void *state;
  channel_c_state_destroy_fn state_destroy;
  void *state_destroy_user_data;

  void *transport_ctx;
  channel_c_transport_kind_t transport_kind;
  channel_c_transport_ops_t transport_ops;
};

static failpoint_entry_t g_failpoints[64];
static pump_entry_t *g_pumps = NULL;
static size_t g_pump_count = 0;
static size_t g_pump_capacity = 0;
static int g_is_pumping = 0;

static int64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64_t)ts.tv_sec * 1000LL) + ((int64_t)ts.tv_nsec / 1000000LL);
}

void channel_c_internal_sleep_short(void) {
  struct timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 100000L;
  nanosleep(&ts, NULL);
}

char *channel_c_strdup(const char *source) {
  size_t length;
  char *copy;

  if (!source) return NULL;
  length = strlen(source);
  copy = (char *)malloc(length + 1);
  if (!copy) return NULL;
  memcpy(copy, source, length + 1);
  return copy;
}

void channel_c_free(void *pointer) {
  free(pointer);
}

void channel_c_internal_failpoint_reset(void) {
  size_t index;

  for (index = 0; index < sizeof(g_failpoints) / sizeof(g_failpoints[0]); index += 1) {
    free(g_failpoints[index].name);
    g_failpoints[index].name = NULL;
    g_failpoints[index].remaining = 0;
  }
}

void channel_c_internal_failpoint_set(const char *name, int count) {
  size_t index;
  size_t free_slot = sizeof(g_failpoints) / sizeof(g_failpoints[0]);
  char *copy;

  if (!name || count <= 0) return;

  copy = channel_c_strdup(name);
  if (!copy) return;

  for (index = 0; index < sizeof(g_failpoints) / sizeof(g_failpoints[0]); index += 1) {
    if (g_failpoints[index].name && strcmp(g_failpoints[index].name, name) == 0) {
      free(g_failpoints[index].name);
      g_failpoints[index].name = copy;
      g_failpoints[index].remaining = count;
      return;
    }
    if (!g_failpoints[index].name && free_slot == sizeof(g_failpoints) / sizeof(g_failpoints[0])) {
      free_slot = index;
    }
  }

  if (free_slot < sizeof(g_failpoints) / sizeof(g_failpoints[0])) {
    g_failpoints[free_slot].name = copy;
    g_failpoints[free_slot].remaining = count;
    return;
  }

  free(copy);
}

int channel_c_internal_failpoint_hit(const char *name) {
  size_t index;

  if (!name) return 0;

  for (index = 0; index < sizeof(g_failpoints) / sizeof(g_failpoints[0]); index += 1) {
    if (!g_failpoints[index].name) continue;
    if (strcmp(g_failpoints[index].name, name) != 0) continue;
    if (g_failpoints[index].remaining <= 0) return 0;
    g_failpoints[index].remaining -= 1;
    return 1;
  }

  return 0;
}

static void pump_compact(void) {
  while (g_pump_count > 0 && g_pumps[g_pump_count - 1].ctx == NULL) {
    g_pump_count -= 1;
  }
}

void channel_c_internal_register_pump(void *ctx, channel_c_poll_fn poll_fn) {
  size_t index;
  pump_entry_t *resized;
  size_t next_capacity;

  if (!ctx || !poll_fn) return;

  for (index = 0; index < g_pump_count; index += 1) {
    if (g_pumps[index].ctx == NULL) {
      g_pumps[index].ctx = ctx;
      g_pumps[index].poll_fn = poll_fn;
      return;
    }
  }

  if (g_pump_count < g_pump_capacity) {
    g_pumps[g_pump_count].ctx = ctx;
    g_pumps[g_pump_count].poll_fn = poll_fn;
    g_pump_count += 1;
    return;
  }

  next_capacity = g_pump_capacity == 0 ? 8 : g_pump_capacity * 2;
  resized = (pump_entry_t *)realloc(g_pumps, next_capacity * sizeof(pump_entry_t));
  if (!resized) return;

  g_pumps = resized;
  memset(g_pumps + g_pump_capacity, 0, (next_capacity - g_pump_capacity) * sizeof(pump_entry_t));
  g_pump_capacity = next_capacity;
  g_pumps[g_pump_count].ctx = ctx;
  g_pumps[g_pump_count].poll_fn = poll_fn;
  g_pump_count += 1;
}

void channel_c_internal_unregister_pump(void *ctx) {
  size_t index;

  if (!ctx) return;

  for (index = 0; index < g_pump_count; index += 1) {
    if (g_pumps[index].ctx == ctx) {
      g_pumps[index].ctx = NULL;
      g_pumps[index].poll_fn = NULL;
      break;
    }
  }

  if (!g_is_pumping) pump_compact();
}

int channel_c_internal_pump_all(void) {
  size_t index;
  int progressed = 0;

  if (g_is_pumping) return 0;
  g_is_pumping = 1;

  for (index = 0; index < g_pump_count; index += 1) {
    int result;
    if (!g_pumps[index].ctx || !g_pumps[index].poll_fn) continue;
    result = g_pumps[index].poll_fn(g_pumps[index].ctx);
    if (result > 0) progressed = 1;
  }

  g_is_pumping = 0;
  pump_compact();
  return progressed;
}

static void free_stream_items(pending_stream_item_t *item) {
  while (item) {
    pending_stream_item_t *next = item->next;
    free(item->body_json);
    free(item->error_text);
    free(item);
    item = next;
  }
}

static void free_listener_callbacks(listener_callback_t *callback) {
  while (callback) {
    listener_callback_t *next = callback->next;
    free(callback);
    callback = next;
  }
}

static void free_listeners(listener_entry_t *listener) {
  while (listener) {
    listener_entry_t *next = listener->next;
    free(listener->topic);
    free_listener_callbacks(listener->callbacks);
    free(listener);
    listener = next;
  }
}

static void free_topics(topic_interest_t *topic) {
  while (topic) {
    topic_interest_t *next = topic->next;
    free(topic->topic);
    free(topic);
    topic = next;
  }
}

static void free_active_streams(active_stream_t *stream) {
  while (stream) {
    active_stream_t *next = stream->next;
    free(stream);
    stream = next;
  }
}

static void pending_dispose(pending_request_t *pending) {
  if (!pending) return;
  free(pending->body_json);
  free(pending->error_text);
  free(pending->topic);
  free_stream_items(pending->stream_head);
  free(pending);
}

static pending_request_t *pending_find(channel_c_connection_t *connection, int id) {
  pending_request_t *current = connection ? connection->pending : NULL;
  while (current) {
    if (current->id == id) return current;
    current = current->next;
  }
  return NULL;
}

static void pending_remove(channel_c_connection_t *connection, int id) {
  pending_request_t *previous = NULL;
  pending_request_t *current;

  if (!connection) return;

  current = connection->pending;
  while (current) {
    if (current->id == id) {
      if (previous) {
        previous->next = current->next;
      } else {
        connection->pending = current->next;
      }
      pending_dispose(current);
      return;
    }
    previous = current;
    current = current->next;
  }
}

static int pending_append_stream_item(pending_request_t *pending, char *body_json, char *error_text, int done) {
  pending_stream_item_t *item;

  if (!pending) return -1;

  item = (pending_stream_item_t *)calloc(1, sizeof(pending_stream_item_t));
  if (!item) {
    free(body_json);
    free(error_text);
    return -1;
  }

  item->body_json = body_json;
  item->error_text = error_text;
  item->done = done;

  if (pending->stream_tail) {
    pending->stream_tail->next = item;
  } else {
    pending->stream_head = item;
  }
  pending->stream_tail = item;
  return 0;
}

static pending_stream_item_t *pending_pop_stream_item(pending_request_t *pending) {
  pending_stream_item_t *item;

  if (!pending || !pending->stream_head) return NULL;
  item = pending->stream_head;
  pending->stream_head = item->next;
  if (!pending->stream_head) pending->stream_tail = NULL;
  item->next = NULL;
  return item;
}

static listener_entry_t *find_listener(listener_entry_t *listeners, const char *topic) {
  while (listeners) {
    if (strcmp(listeners->topic, topic) == 0) return listeners;
    listeners = listeners->next;
  }
  return NULL;
}

static topic_interest_t *find_topic(topic_interest_t *topics, const char *topic) {
  while (topics) {
    if (strcmp(topics->topic, topic) == 0) return topics;
    topics = topics->next;
  }
  return NULL;
}

static int topic_set_contains(topic_interest_t *topics, const char *topic) {
  return find_topic(topics, topic) != NULL;
}

static int connection_uses_process_transport(channel_c_connection_t *connection) {
  return connection && connection->transport_kind == CHANNEL_C_TRANSPORT_PROCESS;
}

static int sender_send_direct(
  channel_c_connection_t *connection,
  pending_request_t *pending,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json,
  char **out_error
);

static int topic_set_add(topic_interest_t **topics, const char *topic) {
  topic_interest_t *entry;

  if (!topics || !topic) return -1;
  if (topic_set_contains(*topics, topic)) return 0;

  entry = (topic_interest_t *)calloc(1, sizeof(topic_interest_t));
  if (!entry) return -1;

  entry->topic = channel_c_strdup(topic);
  if (!entry->topic) {
    free(entry);
    return -1;
  }

  entry->next = *topics;
  *topics = entry;
  return 0;
}

static void topic_set_remove(topic_interest_t **topics, const char *topic) {
  topic_interest_t *previous = NULL;
  topic_interest_t *current;

  if (!topics || !topic) return;

  current = *topics;
  while (current) {
    if (strcmp(current->topic, topic) == 0) {
      if (previous) {
        previous->next = current->next;
      } else {
        *topics = current->next;
      }
      free(current->topic);
      free(current);
      return;
    }
    previous = current;
    current = current->next;
  }
}

static active_stream_t *active_stream_find(active_stream_t *streams, int id) {
  while (streams) {
    if (streams->id == id) return streams;
    streams = streams->next;
  }
  return NULL;
}

static void active_stream_remove(active_stream_t **streams, int id) {
  active_stream_t *previous = NULL;
  active_stream_t *current;

  if (!streams) return;
  current = *streams;
  while (current) {
    if (current->id == id) {
      if (previous) {
        previous->next = current->next;
      } else {
        *streams = current->next;
      }
      free(current);
      return;
    }
    previous = current;
    current = current->next;
  }
}

static post_entry_t *find_post_handler(channel_c_channel_t *channel, const char *path) {
  post_entry_t *entry = channel ? channel->posts : NULL;
  while (entry) {
    if (strcmp(entry->path, path) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static stream_entry_t *find_stream_handler(channel_c_channel_t *channel, const char *path) {
  stream_entry_t *entry = channel ? channel->streams : NULL;
  while (entry) {
    if (strcmp(entry->path, path) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static subscription_entry_t *find_subscription(channel_c_channel_t *channel, const char *name) {
  subscription_entry_t *entry = channel ? channel->subscriptions : NULL;
  while (entry) {
    if (strcmp(entry->name, name) == 0) return entry;
    entry = entry->next;
  }
  return NULL;
}

static char *json_write_value(yyjson_val *value) {
  if (!value) return NULL;
  return yyjson_val_write(value, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
}

static yyjson_mut_val *json_copy_value_from_text(yyjson_mut_doc *doc, const char *json_text) {
  yyjson_doc *source_doc;
  yyjson_val *source_root;
  yyjson_mut_val *copied;

  if (!doc || !json_text) return NULL;

  source_doc = yyjson_read(json_text, strlen(json_text), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!source_doc) return NULL;

  source_root = yyjson_doc_get_root(source_doc);
  copied = yyjson_val_mut_copy(doc, source_root);
  yyjson_doc_free(source_doc);
  return copied;
}

static char *json_write_string_text(const char *text) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  char *json;

  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = text ? yyjson_mut_strcpy(doc, text) : yyjson_mut_null(doc);
  yyjson_mut_doc_set_root(doc, root);
  json = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return json;
}

static char *json_read_string_text(const char *json_text) {
  yyjson_doc *doc;
  yyjson_val *root;
  char *text = NULL;

  if (!json_text) return NULL;

  doc = yyjson_read(json_text, strlen(json_text), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!doc) return NULL;

  root = yyjson_doc_get_root(doc);
  if (root && yyjson_is_str(root)) {
    text = channel_c_strdup(yyjson_get_str(root));
  }

  yyjson_doc_free(doc);
  return text;
}

static char *json_message_response(
  int id,
  const char *body_json,
  const char *error_text,
  const char *topic,
  int include_done,
  int done_value
) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  yyjson_mut_val *body;
  char *text;

  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "id", id);

  if (topic) yyjson_mut_obj_add_strcpy(doc, root, "topic", topic);
  if (error_text) {
    yyjson_mut_obj_add_strcpy(doc, root, "error", error_text);
  } else if (body_json) {
    body = json_copy_value_from_text(doc, body_json);
    if (!body) {
      yyjson_mut_doc_free(doc);
      return NULL;
    }
    yyjson_mut_obj_add_val(doc, root, "body", body);
  }

  if (include_done) yyjson_mut_obj_add_bool(doc, root, "done", done_value ? 1 : 0);

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_event(const char *topic, const char *body_json) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  yyjson_mut_val *body;
  char *text;

  if (!topic || !body_json) return NULL;

  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_strcpy(doc, root, "topic", topic);

  body = json_copy_value_from_text(doc, body_json);
  if (!body) {
    yyjson_mut_doc_free(doc);
    return NULL;
  }
  yyjson_mut_obj_add_val(doc, root, "body", body);

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_request(
  int id,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json
) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  yyjson_mut_val *body;
  yyjson_mut_val *context;
  char *text;

  if (!field || !name) return NULL;

  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "id", id);
  yyjson_mut_obj_add_strcpy(doc, root, field, name);

  if (body_json) {
    body = json_copy_value_from_text(doc, body_json);
    if (!body) {
      yyjson_mut_doc_free(doc);
      return NULL;
    }
    yyjson_mut_obj_add_val(doc, root, "body", body);
  }

  if (context_json) {
    context = json_copy_value_from_text(doc, context_json);
    if (!context) {
      yyjson_mut_doc_free(doc);
      return NULL;
    }
    yyjson_mut_obj_add_val(doc, root, "context", context);
  }

  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_unsubscribe(const char *topic) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  char *text;

  if (!topic) return NULL;
  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_strcpy(doc, root, "unsub", topic);
  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *json_message_cancel(int id) {
  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
  char *text;

  doc = yyjson_mut_doc_new(NULL);
  if (!doc) return NULL;

  root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);
  yyjson_mut_obj_add_int(doc, root, "cancel", id);
  text = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL);
  yyjson_mut_doc_free(doc);
  return text;
}

static char *join_prefix(const char *prefix, const char *name) {
  size_t length;
  char *result;

  if (!prefix || prefix[0] == '\0') return channel_c_strdup(name);
  length = strlen(prefix) + strlen(name) + 2;
  result = (char *)malloc(length);
  if (!result) return NULL;
  snprintf(result, length, "%s/%s", prefix, name);
  return result;
}

static char *subscription_topic_from_body(const char *request_body_json) {
  yyjson_doc *doc;
  yyjson_val *root;
  char *topic;

  if (!request_body_json) return channel_c_strdup("");

  doc = yyjson_read(request_body_json, strlen(request_body_json), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!doc) return channel_c_strdup(request_body_json);

  root = yyjson_doc_get_root(doc);
  if (root && yyjson_is_str(root)) {
    topic = channel_c_strdup(yyjson_get_str(root));
  } else {
    topic = channel_c_strdup(request_body_json);
  }

  yyjson_doc_free(doc);
  return topic;
}

static void channel_attach_connection(channel_c_channel_t *channel, channel_c_connection_t *connection) {
  if (!channel || !connection) return;
  connection->channel_next = channel->connections;
  channel->connections = connection;
  channel_c_connection_retain(connection);
}

static void channel_detach_connection(channel_c_connection_t *connection) {
  channel_c_connection_t *previous = NULL;
  channel_c_connection_t *current;
  channel_c_channel_t *channel;

  if (!connection || !connection->channel) return;
  channel = connection->channel;

  current = channel->connections;
  while (current) {
    if (current == connection) {
      if (previous) {
        previous->channel_next = current->channel_next;
      } else {
        channel->connections = current->channel_next;
      }
      connection->channel = NULL;
      connection->channel_next = NULL;
      channel_c_connection_release(connection);
      return;
    }
    previous = current;
    current = current->channel_next;
  }

  connection->channel = NULL;
  connection->channel_next = NULL;
}

void channel_c_connection_retain(channel_c_connection_t *connection) {
  if (!connection) return;
  connection->ref_count += 1;
}

static void connection_fail_all_pending(channel_c_connection_t *connection) {
  pending_request_t *pending = connection ? connection->pending : NULL;
  while (pending) {
    switch (pending->kind) {
      case PENDING_CALL:
      case PENDING_SUBSCRIPTION:
        if (!pending->completed) {
          free(pending->error_text);
          pending->error_text = channel_c_strdup("disconnected");
          pending->completed = 1;
        }
        break;
      case PENDING_STREAM:
        if (!pending->stream_finished) {
          (void)pending_append_stream_item(pending, NULL, channel_c_strdup("disconnected"), 0);
          pending->stream_finished = 1;
        }
        break;
    }
    pending = pending->next;
  }
}

void channel_c_connection_release(channel_c_connection_t *connection) {
  pending_request_t *pending;

  if (!connection) return;
  connection->ref_count -= 1;
  if (connection->ref_count > 0) return;

  if (connection->state_destroy) {
    connection->state_destroy(connection->state, connection->state_destroy_user_data);
  }

  pending = connection->pending;
  while (pending) {
    pending_request_t *next = pending->next;
    pending_dispose(pending);
    pending = next;
  }

  free_listeners(connection->listeners);
  free_topics(connection->remote_topics);
  free_active_streams(connection->active_streams);
  free(connection);
}

channel_c_connection_t *channel_c_connection_create(
  channel_c_channel_t *channel,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  void *state_destroy_user_data,
  void *transport_ctx,
  channel_c_transport_kind_t transport_kind,
  channel_c_transport_ops_t transport_ops
) {
  channel_c_connection_t *connection;

  if (!channel) return NULL;
  if (channel_c_internal_failpoint_hit("core.connection_alloc_fail")) return NULL;

  connection = (channel_c_connection_t *)calloc(1, sizeof(channel_c_connection_t));
  if (!connection) return NULL;

  connection->ref_count = 1;
  connection->connected = 0;
  connection->closed = 0;
  connection->next_request_id = 1;
  connection->next_callback_id = 1;
  connection->channel = channel;
  connection->sender.connection = connection;
  connection->state = state;
  connection->state_destroy = state_destroy;
  connection->state_destroy_user_data = state_destroy_user_data;
  connection->transport_ctx = transport_ctx;
  connection->transport_kind = transport_kind;
  connection->transport_ops = transport_ops;

  channel_attach_connection(channel, connection);
  return connection;
}

void channel_c_connection_mark_connected(channel_c_connection_t *connection, int connected) {
  if (!connection) return;
  connection->connected = connected ? 1 : 0;
}

channel_c_sender_t *channel_c_connection_sender(channel_c_connection_t *connection) {
  if (!connection) return NULL;
  return &connection->sender;
}

int channel_c_connection_is_connected(channel_c_connection_t *connection) {
  if (!connection) return 0;
  return connection->connected && !connection->closed;
}

int channel_c_connection_is_closed(channel_c_connection_t *connection) {
  if (!connection) return 1;
  return connection->closed;
}

int channel_c_connection_is_remote_subscribed(channel_c_connection_t *connection, const char *topic) {
  if (!connection || !topic) return 0;
  return topic_set_contains(connection->remote_topics, topic);
}

static int connection_transport_send(channel_c_connection_t *connection, const char *text) {
  if (!connection || !text || connection->closed || !connection->transport_ops.send_text) return -1;
  if (connection->transport_ops.send_text(connection->transport_ctx, text) != 0) {
    channel_c_connection_on_close(connection);
    return -1;
  }
  return 0;
}

void channel_c_connection_on_close(channel_c_connection_t *connection) {
  if (!connection || connection->closed) return;
  connection->closed = 1;
  connection->connected = 0;
  connection_fail_all_pending(connection);

  {
    active_stream_t *active = connection->active_streams;
    while (active) {
      active->cancelled = 1;
      active = active->next;
    }
  }

  if (connection->channel) channel_detach_connection(connection);
}

void channel_c_connection_stop(channel_c_connection_t *connection) {
  if (!connection) return;
  if (!connection->closed && connection->transport_ops.close) {
    connection->transport_ops.close(connection->transport_ctx);
  }
  channel_c_connection_on_close(connection);
}

static void connection_dispatch_event(channel_c_connection_t *connection, const char *topic, const char *body_json) {
  listener_entry_t *listener;
  size_t count = 0;
  size_t index = 0;
  struct callback_snapshot {
    channel_c_event_callback_fn callback;
    void *user_data;
  } *snapshots = NULL;

  listener_callback_t *callback;

  if (!connection || !topic || !body_json) return;

  listener = find_listener(connection->listeners, topic);
  if (!listener) return;

  callback = listener->callbacks;
  while (callback) {
    count += 1;
    callback = callback->next;
  }

  if (count == 0) return;
  snapshots = (struct callback_snapshot *)calloc(count, sizeof(*snapshots));
  if (!snapshots) return;

  callback = listener->callbacks;
  while (callback) {
    snapshots[index].callback = callback->callback;
    snapshots[index].user_data = callback->user_data;
    index += 1;
    callback = callback->next;
  }

  for (index = 0; index < count; index += 1) {
    snapshots[index].callback(body_json, snapshots[index].user_data);
  }

  free(snapshots);
}

static void connection_publish_event(channel_c_connection_t *connection, const char *topic, const char *body_json) {
  char *message;

  if (!connection || !topic || !body_json) return;
  if (connection->closed) return;
  if (!topic_set_contains(connection->remote_topics, topic)) return;
  if (connection_uses_process_transport(connection)) {
    connection_dispatch_event(connection, topic, body_json);
    return;
  }

  message = json_message_event(topic, body_json);
  if (!message) return;
  (void)connection_transport_send(connection, message);
  free(message);
}

static int wait_for_pending_completion(channel_c_connection_t *connection, pending_request_t *pending) {
  if (!connection || !pending) return -1;

  while (!pending->completed && !connection->closed) {
    if (!channel_c_internal_pump_all()) channel_c_internal_sleep_short();
  }

  return pending->completed ? 0 : -1;
}

static int wait_for_connection_state(
  channel_c_connection_t *connection,
  int want_connected,
  int timeout_ms
) {
  int64_t deadline;

  if (!connection) return -1;
  deadline = timeout_ms < 0 ? -1 : now_ms() + timeout_ms;

  while (!connection->closed && connection->connected != want_connected) {
    if (!channel_c_internal_pump_all()) channel_c_internal_sleep_short();
    if (deadline >= 0 && now_ms() > deadline) return -1;
  }

  return connection->connected == want_connected ? 0 : -1;
}

static int sender_send_common(
  channel_c_sender_t *sender,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json,
  pending_kind_t kind,
  pending_request_t **out_pending,
  char **out_error
) {
  channel_c_connection_t *connection;
  pending_request_t *pending;
  char *message;

  if (out_error) *out_error = NULL;
  if (out_pending) *out_pending = NULL;

  if (!sender || !field || !name) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  connection = sender->connection;
  if (!connection || connection->closed || !connection->connected) {
    if (out_error) *out_error = channel_c_strdup("connection is not available");
    return -1;
  }

  if (channel_c_internal_failpoint_hit("core.pending_alloc_fail")) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return -1;
  }

  pending = (pending_request_t *)calloc(1, sizeof(pending_request_t));
  if (!pending) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    return -1;
  }

  pending->id = connection->next_request_id++;
  pending->kind = kind;
  pending->next = connection->pending;
  connection->pending = pending;

  if (connection_uses_process_transport(connection)) {
    if (sender_send_direct(connection, pending, field, name, body_json, context_json, out_error) != 0) {
      return -1;
    }
    if (out_pending) *out_pending = pending;
    return 0;
  }

  if (channel_c_internal_failpoint_hit("core.request_json_fail")) {
    message = NULL;
  } else {
    message = json_message_request(pending->id, field, name, body_json, context_json);
  }

  if (!message) {
    pending_remove(connection, pending->id);
    if (out_error) *out_error = channel_c_strdup("invalid json body or context");
    return -1;
  }

  if (connection_transport_send(connection, message) != 0) {
    free(message);
    pending_remove(connection, pending->id);
    if (out_error) *out_error = channel_c_strdup("failed to send request");
    return -1;
  }

  free(message);
  if (out_pending) *out_pending = pending;
  return 0;
}

static void process_topic_message(
  channel_c_connection_t *connection,
  int request_id,
  const char *topic,
  const char *body_json,
  const char *error_text
) {
  pending_request_t *pending;

  pending = request_id >= 0 ? pending_find(connection, request_id) : NULL;
  if (pending && pending->kind == PENDING_SUBSCRIPTION && !pending->completed) {
    if (topic) {
      free(pending->topic);
      pending->topic = channel_c_strdup(topic);
    }
    if (body_json) {
      free(pending->body_json);
      pending->body_json = channel_c_strdup(body_json);
    }
    if (error_text) {
      free(pending->error_text);
      pending->error_text = channel_c_strdup(error_text);
    }
    pending->completed = 1;
  }

  if (body_json) connection_dispatch_event(connection, topic, body_json);
}

static void process_response_message(
  channel_c_connection_t *connection,
  int request_id,
  const char *body_json,
  const char *error_text,
  int has_done,
  int done_value
) {
  pending_request_t *pending;

  if (!connection || request_id < 0) return;
  pending = pending_find(connection, request_id);
  if (!pending) return;

  switch (pending->kind) {
    case PENDING_CALL:
      if (body_json) {
        free(pending->body_json);
        pending->body_json = channel_c_strdup(body_json);
      }
      if (error_text) {
        free(pending->error_text);
        pending->error_text = channel_c_strdup(error_text);
      }
      pending->completed = 1;
      break;
    case PENDING_SUBSCRIPTION:
      if (body_json) {
        free(pending->body_json);
        pending->body_json = channel_c_strdup(body_json);
      }
      if (error_text) {
        free(pending->error_text);
        pending->error_text = channel_c_strdup(error_text);
      }
      pending->completed = 1;
      break;
    case PENDING_STREAM:
      (void)pending_append_stream_item(
        pending,
        body_json ? channel_c_strdup(body_json) : NULL,
        error_text ? channel_c_strdup(error_text) : NULL,
        has_done ? done_value : 0
      );
      if (error_text || (has_done && done_value)) pending->stream_finished = 1;
      break;
  }
}

static void dispatch_post_request_direct(
  channel_c_connection_t *connection,
  int request_id,
  const char *path,
  const char *body_json,
  const char *context_json
) {
  post_entry_t *entry;
  channel_c_post_request_t request;
  char *response_json = NULL;
  char *error_text = NULL;
  int rc = -1;

  if (!connection || !path) return;

  entry = find_post_handler(connection->channel, path);
  if (!entry) {
    if (request_id >= 0 && !connection->closed) {
      process_response_message(connection, request_id, NULL, "api not found", 0, 0);
    }
    return;
  }

  request.path = path;
  request.body_json = (entry->flags & CHANNEL_C_POST_IGNORE_BODY) ? NULL : body_json;
  request.context_json = (entry->flags & CHANNEL_C_POST_IGNORE_CONTEXT) ? NULL : context_json;
  request.sender = channel_c_connection_sender(connection);
  request.state = connection->state;
  request.user_data = entry->user_data;

  rc = entry->handler(&request, &response_json, &error_text);
  if (rc != 0 && !error_text) error_text = channel_c_strdup("handler failed");

  if (request_id >= 0 && !connection->closed) {
    process_response_message(connection, request_id, response_json, error_text, 0, 0);
  }

  free(response_json);
  free(error_text);
}

static void dispatch_stream_request_direct(
  channel_c_connection_t *connection,
  int request_id,
  const char *path,
  const char *body_json,
  const char *context_json
) {
  stream_entry_t *entry;
  channel_c_stream_request_t request;
  channel_c_stream_emitter_t emitter;
  active_stream_t *active;

  if (!connection || !path || request_id < 0) return;

  entry = find_stream_handler(connection->channel, path);
  if (!entry) {
    if (!connection->closed) process_response_message(connection, request_id, NULL, "api not found", 0, 0);
    return;
  }

  active = (active_stream_t *)calloc(1, sizeof(active_stream_t));
  if (!active) {
    if (!connection->closed) process_response_message(connection, request_id, NULL, "internal error", 0, 0);
    return;
  }

  active->id = request_id;
  active->cancelled = 0;
  active->next = connection->active_streams;
  connection->active_streams = active;

  emitter.connection = connection;
  emitter.request_id = request_id;
  emitter.active = active;

  request.path = path;
  request.body_json = body_json;
  request.context_json = context_json;
  request.sender = channel_c_connection_sender(connection);
  request.state = connection->state;
  request.user_data = entry->user_data;
  request.emitter = &emitter;

  entry->handler(&request);

  if (!connection->closed) process_response_message(connection, request_id, NULL, NULL, 1, 1);
  active_stream_remove(&connection->active_streams, request_id);
}

static void dispatch_subscription_request_direct(
  channel_c_connection_t *connection,
  int request_id,
  const char *name,
  const char *body_json
) {
  subscription_entry_t *entry;
  channel_c_subscription_request_t request;
  char *topic = NULL;
  char *resolved_body = NULL;
  char *error_text = NULL;
  char *full_topic = NULL;

  if (!connection || !name || request_id < 0) return;

  entry = find_subscription(connection->channel, name);
  if (!entry) {
    if (!connection->closed) process_response_message(connection, request_id, NULL, "subscription not found", 0, 0);
    return;
  }

  if (entry->resolver) {
    request.name = entry->name;
    request.request_body_json = body_json;
    request.sender = channel_c_connection_sender(connection);
    request.state = connection->state;
    request.user_data = entry->user_data;
    if (entry->resolver(&request, &topic, &resolved_body, &error_text) != 0 && !error_text) {
      error_text = channel_c_strdup("subscription failed");
    }
  } else {
    topic = subscription_topic_from_body(body_json);
  }

  if (!topic) topic = channel_c_strdup("");
  if (!error_text) full_topic = join_prefix(entry->prefix, topic);

  if (!connection->closed) {
    if (!error_text && full_topic) {
      (void)topic_set_add(&connection->remote_topics, full_topic);
      process_topic_message(connection, request_id, full_topic, resolved_body, NULL);
    } else if (error_text) {
      process_response_message(connection, request_id, NULL, error_text, 0, 0);
    } else {
      process_response_message(connection, request_id, NULL, "internal error", 0, 0);
    }
  }

  free(topic);
  free(resolved_body);
  free(error_text);
  free(full_topic);
}

static int sender_send_direct(
  channel_c_connection_t *connection,
  pending_request_t *pending,
  const char *field,
  const char *name,
  const char *body_json,
  const char *context_json,
  char **out_error
) {
  if (!connection || !pending || !field || !name) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  if (strcmp(field, "path") == 0) {
    dispatch_post_request_direct(connection, pending->id, name, body_json, context_json);
    return 0;
  }

  if (strcmp(field, "stream") == 0) {
    dispatch_stream_request_direct(connection, pending->id, name, body_json, context_json);
    return 0;
  }

  if (strcmp(field, "sub") == 0) {
    dispatch_subscription_request_direct(connection, pending->id, name, body_json);
    return 0;
  }

  pending_remove(connection, pending->id);
  if (out_error) *out_error = channel_c_strdup("invalid request type");
  return -1;
}

static void dispatch_post_request(
  channel_c_connection_t *connection,
  int request_id,
  const char *path,
  yyjson_val *body_val,
  yyjson_val *context_val
) {
  post_entry_t *entry;
  channel_c_post_request_t request;
  char *body_json = NULL;
  char *context_json = NULL;
  char *response_json = NULL;
  char *error_text = NULL;
  char *message;
  int rc = -1;

  if (!connection || !path) return;
  entry = find_post_handler(connection->channel, path);
  if (!entry) {
    if (request_id >= 0) {
      message = json_message_response(request_id, NULL, "api not found", NULL, 0, 0);
      if (message) {
        (void)connection_transport_send(connection, message);
        free(message);
      }
    }
    return;
  }

  if (!(entry->flags & CHANNEL_C_POST_IGNORE_BODY) && body_val) body_json = json_write_value(body_val);
  if (!(entry->flags & CHANNEL_C_POST_IGNORE_CONTEXT) && context_val) context_json = json_write_value(context_val);

  request.path = path;
  request.body_json = body_json;
  request.context_json = context_json;
  request.sender = channel_c_connection_sender(connection);
  request.state = connection->state;
  request.user_data = entry->user_data;

  rc = entry->handler(&request, &response_json, &error_text);
  if (rc != 0 && !error_text) error_text = channel_c_strdup("handler failed");

  if (request_id >= 0) {
    message = json_message_response(request_id, response_json, error_text, NULL, 0, 0);
    if (message) {
      (void)connection_transport_send(connection, message);
      free(message);
    }
  }

  free(body_json);
  free(context_json);
  free(response_json);
  free(error_text);
}

static void dispatch_stream_request(
  channel_c_connection_t *connection,
  int request_id,
  const char *path,
  yyjson_val *body_val,
  yyjson_val *context_val
) {
  stream_entry_t *entry;
  channel_c_stream_request_t request;
  channel_c_stream_emitter_t emitter;
  active_stream_t *active;
  char *body_json = NULL;
  char *context_json = NULL;
  char *message;

  if (!connection || !path || request_id < 0) return;

  entry = find_stream_handler(connection->channel, path);
  if (!entry) {
    message = json_message_response(request_id, NULL, "api not found", NULL, 0, 0);
    if (message) {
      (void)connection_transport_send(connection, message);
      free(message);
    }
    return;
  }

  active = (active_stream_t *)calloc(1, sizeof(active_stream_t));
  if (!active) {
    message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
    if (message) {
      (void)connection_transport_send(connection, message);
      free(message);
    }
    return;
  }

  active->id = request_id;
  active->cancelled = 0;
  active->next = connection->active_streams;
  connection->active_streams = active;

  if (body_val) body_json = json_write_value(body_val);
  if (context_val) context_json = json_write_value(context_val);

  emitter.connection = connection;
  emitter.request_id = request_id;
  emitter.active = active;

  request.path = path;
  request.body_json = body_json;
  request.context_json = context_json;
  request.sender = channel_c_connection_sender(connection);
  request.state = connection->state;
  request.user_data = entry->user_data;
  request.emitter = &emitter;

  entry->handler(&request);

  message = json_message_response(request_id, NULL, NULL, NULL, 1, 1);
  if (message) {
    (void)connection_transport_send(connection, message);
    free(message);
  }

  active_stream_remove(&connection->active_streams, request_id);
  free(body_json);
  free(context_json);
}

static void dispatch_subscription_request(
  channel_c_connection_t *connection,
  int request_id,
  const char *name,
  yyjson_val *body_val
) {
  subscription_entry_t *entry;
  channel_c_subscription_request_t request;
  char *request_body_json = NULL;
  char *topic = NULL;
  char *resolved_body = NULL;
  char *error_text = NULL;
  char *full_topic = NULL;
  char *message;

  if (!connection || !name || request_id < 0) return;

  entry = find_subscription(connection->channel, name);
  if (!entry) {
    message = json_message_response(request_id, NULL, "subscription not found", NULL, 0, 0);
    if (message) {
      (void)connection_transport_send(connection, message);
      free(message);
    }
    return;
  }

  if (body_val) request_body_json = json_write_value(body_val);

  if (entry->resolver) {
    request.name = entry->name;
    request.request_body_json = request_body_json;
    request.sender = channel_c_connection_sender(connection);
    request.state = connection->state;
    request.user_data = entry->user_data;
    if (entry->resolver(&request, &topic, &resolved_body, &error_text) != 0 && !error_text) {
      error_text = channel_c_strdup("subscription failed");
    }
  } else {
    topic = subscription_topic_from_body(request_body_json);
  }

  if (!topic) topic = channel_c_strdup("");
  if (!error_text) full_topic = join_prefix(entry->prefix, topic);

  if (!error_text && full_topic) {
    (void)topic_set_add(&connection->remote_topics, full_topic);
    message = json_message_response(request_id, resolved_body, NULL, full_topic, 0, 0);
  } else if (error_text) {
    message = json_message_response(request_id, NULL, error_text, NULL, 0, 0);
  } else {
    message = json_message_response(request_id, NULL, "internal error", NULL, 0, 0);
  }

  if (message) {
    (void)connection_transport_send(connection, message);
    free(message);
  }

  free(request_body_json);
  free(topic);
  free(resolved_body);
  free(error_text);
  free(full_topic);
}

static void process_object(channel_c_connection_t *connection, yyjson_val *object) {
  yyjson_val *id_val;
  yyjson_val *path_val;
  yyjson_val *stream_val;
  yyjson_val *sub_val;
  yyjson_val *unsub_val;
  yyjson_val *cancel_val;
  yyjson_val *topic_val;
  yyjson_val *body_val;
  yyjson_val *context_val;
  yyjson_val *error_val;
  yyjson_val *done_val;
  int request_id = -1;
  char *body_json = NULL;
  char *error_text = NULL;
  int has_done = 0;
  int done_value = 0;

  if (!connection || !object) return;

  id_val = yyjson_obj_get(object, "id");
  path_val = yyjson_obj_get(object, "path");
  stream_val = yyjson_obj_get(object, "stream");
  sub_val = yyjson_obj_get(object, "sub");
  unsub_val = yyjson_obj_get(object, "unsub");
  cancel_val = yyjson_obj_get(object, "cancel");
  topic_val = yyjson_obj_get(object, "topic");
  body_val = yyjson_obj_get(object, "body");
  context_val = yyjson_obj_get(object, "context");
  error_val = yyjson_obj_get(object, "error");
  done_val = yyjson_obj_get(object, "done");

  if (id_val && (yyjson_is_int(id_val) || yyjson_is_uint(id_val))) {
    request_id = (int)yyjson_get_sint(id_val);
  }
  if (body_val) body_json = json_write_value(body_val);
  if (yyjson_is_str(error_val)) error_text = channel_c_strdup(yyjson_get_str(error_val));
  if (done_val && yyjson_is_bool(done_val)) {
    has_done = 1;
    done_value = yyjson_get_bool(done_val) ? 1 : 0;
  }

  if (yyjson_is_str(topic_val)) {
    process_topic_message(connection, request_id, yyjson_get_str(topic_val), body_json, error_text);
    free(body_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(path_val)) {
    dispatch_post_request(connection, request_id, yyjson_get_str(path_val), body_val, context_val);
    free(body_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(stream_val)) {
    dispatch_stream_request(connection, request_id, yyjson_get_str(stream_val), body_val, context_val);
    free(body_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(sub_val)) {
    dispatch_subscription_request(connection, request_id, yyjson_get_str(sub_val), body_val);
    free(body_json);
    free(error_text);
    return;
  }

  if (yyjson_is_str(unsub_val)) {
    topic_set_remove(&connection->remote_topics, yyjson_get_str(unsub_val));
    free(body_json);
    free(error_text);
    return;
  }

  if (cancel_val && (yyjson_is_int(cancel_val) || yyjson_is_uint(cancel_val))) {
    active_stream_t *active = active_stream_find(connection->active_streams, (int)yyjson_get_sint(cancel_val));
    if (active) active->cancelled = 1;
    free(body_json);
    free(error_text);
    return;
  }

  if (request_id >= 0) {
    process_response_message(connection, request_id, body_json, error_text, has_done, done_value);
  }

  free(body_json);
  free(error_text);
}

void channel_c_connection_receive_text(channel_c_connection_t *connection, const char *text) {
  yyjson_doc *doc;
  yyjson_val *root;
  yyjson_val *item;
  size_t index;
  size_t max;

  if (!connection || !text || connection->closed) return;

  doc = yyjson_read(text, strlen(text), YYJSON_READ_ALLOW_INF_AND_NAN);
  if (!doc) {
    channel_c_connection_on_close(connection);
    return;
  }

  root = yyjson_doc_get_root(doc);
  if (yyjson_is_arr(root)) {
    yyjson_arr_foreach(root, index, max, item) {
      if (yyjson_is_obj(item)) process_object(connection, item);
    }
  } else if (yyjson_is_obj(root)) {
    process_object(connection, root);
  }

  yyjson_doc_free(doc);
}

channel_c_channel_t *channel_c_channel_new(void) {
  if (channel_c_internal_failpoint_hit("core.channel_alloc_fail")) return NULL;
  return (channel_c_channel_t *)calloc(1, sizeof(channel_c_channel_t));
}

void channel_c_channel_free(channel_c_channel_t *channel) {
  post_entry_t *post;
  stream_entry_t *stream;
  subscription_entry_t *subscription;

  if (!channel) return;

  while (channel->connections) {
    channel_c_connection_t *connection = channel->connections;
    channel_c_connection_stop(connection);
  }

  post = channel->posts;
  while (post) {
    post_entry_t *next = post->next;
    free(post->path);
    free(post);
    post = next;
  }

  stream = channel->streams;
  while (stream) {
    stream_entry_t *next = stream->next;
    free(stream->path);
    free(stream);
    stream = next;
  }

  subscription = channel->subscriptions;
  while (subscription) {
    subscription_entry_t *next = subscription->next;
    free(subscription->name);
    free(subscription->prefix);
    free(subscription);
    subscription = next;
  }

  free(channel);
}

int channel_c_channel_post(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_post_handler_fn handler,
  void *user_data,
  unsigned int flags
) {
  post_entry_t *entry;

  if (!channel || !path || !handler) return -1;

  entry = (post_entry_t *)calloc(1, sizeof(post_entry_t));
  if (!entry) return -1;

  entry->path = channel_c_strdup(path);
  if (!entry->path) {
    free(entry);
    return -1;
  }

  entry->path_length = strlen(path);
  entry->handler = handler;
  entry->user_data = user_data;
  entry->flags = flags;
  entry->next = channel->posts;
  channel->posts = entry;
  return 0;
}

int channel_c_channel_stream(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_stream_handler_fn handler,
  void *user_data
) {
  stream_entry_t *entry;

  if (!channel || !path || !handler) return -1;

  entry = (stream_entry_t *)calloc(1, sizeof(stream_entry_t));
  if (!entry) return -1;

  entry->path = channel_c_strdup(path);
  if (!entry->path) {
    free(entry);
    return -1;
  }

  entry->handler = handler;
  entry->user_data = user_data;
  entry->next = channel->streams;
  channel->streams = entry;
  return 0;
}

int channel_c_channel_subscription(
  channel_c_channel_t *channel,
  const char *name,
  channel_c_subscription_resolver_fn resolver,
  void *user_data
) {
  subscription_entry_t *entry;

  if (!channel || !name) return -1;

  entry = (subscription_entry_t *)calloc(1, sizeof(subscription_entry_t));
  if (!entry) return -1;

  entry->name = channel_c_strdup(name);
  entry->prefix = channel_c_strdup(name);
  if (!entry->name || !entry->prefix) {
    free(entry->name);
    free(entry->prefix);
    free(entry);
    return -1;
  }

  entry->resolver = resolver;
  entry->user_data = user_data;
  entry->next = channel->subscriptions;
  channel->subscriptions = entry;
  return 0;
}

int channel_c_channel_merge(channel_c_channel_t *destination, channel_c_channel_t *source, const char *prefix) {
  post_entry_t *post;
  stream_entry_t *stream;
  subscription_entry_t *subscription;

  if (!destination || !source) return -1;

  post = source->posts;
  while (post) {
    char *path = join_prefix(prefix, post->path);
    int rc = path ? channel_c_channel_post(destination, path, post->handler, post->user_data, post->flags) : -1;
    free(path);
    if (rc != 0) return -1;
    post = post->next;
  }

  stream = source->streams;
  while (stream) {
    char *path = join_prefix(prefix, stream->path);
    int rc = path ? channel_c_channel_stream(destination, path, stream->handler, stream->user_data) : -1;
    free(path);
    if (rc != 0) return -1;
    stream = stream->next;
  }

  subscription = source->subscriptions;
  while (subscription) {
    char *name = join_prefix(prefix, subscription->name);
    int rc = name ? channel_c_channel_subscription(destination, name, subscription->resolver, subscription->user_data) : -1;
    free(name);
    if (rc != 0) return -1;
    subscription = subscription->next;
  }

  return 0;
}

int channel_c_channel_event_send(
  channel_c_channel_t *channel,
  const char *subscription_name,
  const char *request_body_json,
  const char *body_override_json,
  char **out_error
) {
  subscription_entry_t *entry;
  char *topic = NULL;
  char *body = NULL;
  char *full_topic = NULL;
  channel_c_connection_t *connection;

  if (out_error) *out_error = NULL;
  if (!channel || !subscription_name) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  entry = find_subscription(channel, subscription_name);
  if (!entry) {
    if (out_error) *out_error = channel_c_strdup("subscription not found");
    return -1;
  }

  if (entry->resolver) {
    channel_c_subscription_request_t request;
    char *resolved_body = NULL;
    char *error_text = NULL;
    request.name = entry->name;
    request.request_body_json = request_body_json;
    request.sender = NULL;
    request.state = NULL;
    request.user_data = entry->user_data;
    if (entry->resolver(&request, &topic, &resolved_body, &error_text) != 0) {
      if (out_error) *out_error = error_text ? error_text : channel_c_strdup("subscription failed");
      free(topic);
      free(resolved_body);
      return -1;
    }
    if (!body_override_json) body = resolved_body;
    else free(resolved_body);
  } else {
    topic = subscription_topic_from_body(request_body_json);
  }

  if (!topic) topic = channel_c_strdup("");
  if (!body && body_override_json) body = channel_c_strdup(body_override_json);
  if (!body) {
    if (out_error) *out_error = channel_c_strdup("body is required");
    free(topic);
    return -1;
  }

  full_topic = join_prefix(entry->prefix, topic);
  if (!full_topic) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    free(topic);
    free(body);
    return -1;
  }

  connection = channel->connections;
  while (connection) {
    channel_c_connection_t *next = connection->channel_next;
    connection_publish_event(connection, full_topic, body);
    connection = next;
  }

  free(topic);
  free(body);
  free(full_topic);
  return 0;
}

static listener_entry_t *ensure_listener(channel_c_connection_t *connection, const char *topic) {
  listener_entry_t *listener;

  listener = find_listener(connection->listeners, topic);
  if (listener) return listener;
  if (channel_c_internal_failpoint_hit("core.listener_alloc_fail")) return NULL;

  listener = (listener_entry_t *)calloc(1, sizeof(listener_entry_t));
  if (!listener) return NULL;
  listener->topic = channel_c_strdup(topic);
  if (!listener->topic) {
    free(listener);
    return NULL;
  }
  listener->next = connection->listeners;
  connection->listeners = listener;
  return listener;
}

int channel_c_sender_send(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_body_json,
  char **out_error
) {
  pending_request_t *pending = NULL;
  channel_c_connection_t *connection;

  if (out_body_json) *out_body_json = NULL;
  if (out_error) *out_error = NULL;

  if (sender_send_common(sender, "path", path, body_json, context_json, PENDING_CALL, &pending, out_error) != 0) {
    return -1;
  }

  connection = sender->connection;
  if (wait_for_pending_completion(connection, pending) != 0) {
    if (out_error && !*out_error) *out_error = channel_c_strdup("connection closed");
    pending_remove(connection, pending->id);
    return -1;
  }

  if (pending->error_text) {
    if (out_error) *out_error = channel_c_strdup(pending->error_text);
    pending_remove(connection, pending->id);
    return -1;
  }

  if (out_body_json && pending->body_json) *out_body_json = channel_c_strdup(pending->body_json);
  pending_remove(connection, pending->id);
  return 0;
}

int channel_c_sender_send_string(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_text,
  char **out_body_text,
  char **out_error
) {
  char *body_json = NULL;
  char *response_json = NULL;
  char *response_text = NULL;
  int rc;

  if (out_body_text) *out_body_text = NULL;
  if (out_error) *out_error = NULL;

  if (body_text) {
    body_json = json_write_string_text(body_text);
    if (!body_json) {
      if (out_error) *out_error = channel_c_strdup("internal error");
      return -1;
    }
  }

  rc = channel_c_sender_send(sender, path, body_json, NULL, &response_json, out_error);
  free(body_json);
  if (rc != 0) return rc;

  if (!response_json) return 0;
  response_text = json_read_string_text(response_json);
  free(response_json);

  if (!response_text) {
    if (out_error) *out_error = channel_c_strdup("response is not a JSON string");
    return -1;
  }

  if (out_body_text) {
    *out_body_text = response_text;
    response_text = NULL;
  }
  free(response_text);
  return 0;
}

channel_c_values_t *channel_c_sender_values(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_error
) {
  pending_request_t *pending = NULL;
  channel_c_values_t *values;
  channel_c_connection_t *connection;

  if (out_error) *out_error = NULL;

  if (sender_send_common(sender, "stream", path, body_json, context_json, PENDING_STREAM, &pending, out_error) != 0) {
    return NULL;
  }

  connection = sender->connection;
  if (channel_c_internal_failpoint_hit("core.values_alloc_fail")) {
    values = NULL;
  } else {
    values = (channel_c_values_t *)calloc(1, sizeof(channel_c_values_t));
  }

  if (!values) {
    pending_remove(connection, pending->id);
    if (out_error) *out_error = channel_c_strdup("internal error");
    return NULL;
  }

  values->connection = connection;
  values->request_id = pending->id;
  values->closed = 0;
  channel_c_connection_retain(connection);
  return values;
}

int channel_c_values_next(channel_c_values_t *values, char **out_body_json, int *out_done, char **out_error) {
  channel_c_connection_t *connection;
  pending_request_t *pending;
  pending_stream_item_t *item;

  if (out_body_json) *out_body_json = NULL;
  if (out_done) *out_done = 0;
  if (out_error) *out_error = NULL;

  if (!values || values->closed) {
    if (out_done) *out_done = 1;
    return 0;
  }

  connection = values->connection;

  while (1) {
    pending = pending_find(connection, values->request_id);
    if (!pending || pending->kind != PENDING_STREAM) {
      values->closed = 1;
      if (out_done) *out_done = 1;
      return 0;
    }
    if (pending->stream_head) break;
    if (pending->stream_finished) {
      values->closed = 1;
      pending_remove(connection, values->request_id);
      if (out_done) *out_done = 1;
      return 0;
    }
    if (connection->closed) {
      if (out_error) *out_error = channel_c_strdup("connection closed");
      return -1;
    }
    if (!channel_c_internal_pump_all()) channel_c_internal_sleep_short();
  }

  item = pending_pop_stream_item(pending);
  if (!item) {
    if (out_done) *out_done = 1;
    return 0;
  }

  if (item->error_text) {
    if (out_error) *out_error = channel_c_strdup(item->error_text);
    free_stream_items(item);
    values->closed = 1;
    pending_remove(connection, values->request_id);
    return -1;
  }

  if (item->done) {
    free(item->body_json);
    free(item->error_text);
    free(item);
    values->closed = 1;
    pending_remove(connection, values->request_id);
    if (out_done) *out_done = 1;
    return 0;
  }

  if (out_body_json && item->body_json) *out_body_json = channel_c_strdup(item->body_json);
  free(item->body_json);
  free(item->error_text);
  free(item);
  return 0;
}

void channel_c_values_close(channel_c_values_t *values) {
  channel_c_connection_t *connection;
  pending_request_t *pending;
  char *message;

  if (!values) return;
  connection = values->connection;

  if (!values->closed) {
    pending = pending_find(connection, values->request_id);
    if (pending) {
      if (connection_uses_process_transport(connection)) {
        active_stream_t *active = active_stream_find(connection->active_streams, values->request_id);
        if (active) active->cancelled = 1;
      } else {
        message = json_message_cancel(values->request_id);
        if (message) {
          (void)connection_transport_send(connection, message);
          free(message);
        }
      }
      pending_remove(connection, values->request_id);
    }
    values->closed = 1;
  }

  channel_c_connection_release(connection);
  free(values);
}

int channel_c_sender_subscribe(
  channel_c_sender_t *sender,
  const char *subscription_name,
  const char *body_json,
  const char *context_json,
  channel_c_event_callback_fn callback,
  void *user_data,
  channel_c_subscription_handle_t **out_handle,
  char **out_error
) {
  channel_c_connection_t *connection;
  pending_request_t *pending = NULL;
  listener_entry_t *listener;
  listener_callback_t *callback_entry;
  channel_c_subscription_handle_t *handle;

  if (out_handle) *out_handle = NULL;
  if (out_error) *out_error = NULL;

  if (!sender || !subscription_name || !callback) {
    if (out_error) *out_error = channel_c_strdup("invalid arguments");
    return -1;
  }

  if (sender_send_common(
        sender,
        "sub",
        subscription_name,
        body_json,
        context_json,
        PENDING_SUBSCRIPTION,
        &pending,
        out_error
      ) != 0) {
    return -1;
  }

  connection = sender->connection;
  if (wait_for_pending_completion(connection, pending) != 0) {
    if (out_error && !*out_error) *out_error = channel_c_strdup("connection closed");
    pending_remove(connection, pending->id);
    return -1;
  }

  if (pending->error_text || !pending->topic) {
    if (out_error) {
      *out_error = channel_c_strdup(pending->error_text ? pending->error_text : "subscription failed");
    }
    pending_remove(connection, pending->id);
    return -1;
  }

  listener = ensure_listener(connection, pending->topic);
  if (!listener) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_remove(connection, pending->id);
    return -1;
  }

  callback_entry = (listener_callback_t *)calloc(1, sizeof(listener_callback_t));
  if (!callback_entry) {
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_remove(connection, pending->id);
    return -1;
  }

  callback_entry->id = connection->next_callback_id++;
  callback_entry->callback = callback;
  callback_entry->user_data = user_data;
  callback_entry->next = listener->callbacks;
  listener->callbacks = callback_entry;

  if (channel_c_internal_failpoint_hit("core.subscription_handle_alloc_fail")) {
    handle = NULL;
  } else {
    handle = (channel_c_subscription_handle_t *)calloc(1, sizeof(channel_c_subscription_handle_t));
  }
  if (!handle) {
    listener->callbacks = callback_entry->next;
    free(callback_entry);
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_remove(connection, pending->id);
    return -1;
  }

  handle->connection = connection;
  handle->topic = channel_c_strdup(pending->topic);
  handle->callback_id = callback_entry->id;
  handle->cancelled = 0;
  if (!handle->topic) {
    free(handle);
    listener->callbacks = callback_entry->next;
    free(callback_entry);
    if (out_error) *out_error = channel_c_strdup("internal error");
    pending_remove(connection, pending->id);
    return -1;
  }

  channel_c_connection_retain(connection);
  if (out_handle) *out_handle = handle;
  pending_remove(connection, pending->id);
  return 0;
}

static int listener_remove_callback(channel_c_connection_t *connection, const char *topic, int callback_id) {
  listener_entry_t *listener;
  listener_entry_t *listener_previous = NULL;
  listener_callback_t *callback;
  listener_callback_t *callback_previous = NULL;

  if (!connection || !topic) return 0;

  listener = connection->listeners;
  while (listener) {
    if (strcmp(listener->topic, topic) == 0) break;
    listener_previous = listener;
    listener = listener->next;
  }
  if (!listener) return 0;

  callback = listener->callbacks;
  while (callback) {
    if (callback->id == callback_id) {
      if (callback_previous) {
        callback_previous->next = callback->next;
      } else {
        listener->callbacks = callback->next;
      }
      free(callback);
      break;
    }
    callback_previous = callback;
    callback = callback->next;
  }

  if (listener->callbacks) return 0;

  if (listener_previous) {
    listener_previous->next = listener->next;
  } else {
    connection->listeners = listener->next;
  }
  free(listener->topic);
  free(listener);
  return 1;
}

void channel_c_subscription_cancel(channel_c_subscription_handle_t *handle) {
  channel_c_connection_t *connection;
  int removed_last = 0;

  if (!handle || handle->cancelled) return;
  handle->cancelled = 1;

  connection = handle->connection;
  removed_last = listener_remove_callback(connection, handle->topic, handle->callback_id);
  if (removed_last && connection && !connection->closed) {
    if (connection_uses_process_transport(connection)) {
      topic_set_remove(&connection->remote_topics, handle->topic);
    } else {
      char *message = json_message_unsubscribe(handle->topic);
      if (message) {
        (void)connection_transport_send(connection, message);
        free(message);
      }
    }
  }

  free(handle->topic);
  channel_c_connection_release(connection);
  free(handle);
}

void channel_c_sender_stop(channel_c_sender_t *sender) {
  if (!sender || !sender->connection) return;
  channel_c_connection_stop(sender->connection);
}

int channel_c_stream_emit_json(channel_c_stream_emitter_t *emitter, const char *body_json) {
  char *message;

  if (!emitter || !body_json) return -1;
  if (!emitter->connection || emitter->connection->closed) return -1;
  if (!emitter->active || emitter->active->cancelled) return -1;

  if (connection_uses_process_transport(emitter->connection)) {
    process_response_message(emitter->connection, emitter->request_id, body_json, NULL, 0, 0);
    return 0;
  }

  message = json_message_response(emitter->request_id, body_json, NULL, NULL, 0, 0);
  if (!message) return -1;
  if (connection_transport_send(emitter->connection, message) != 0) {
    free(message);
    return -1;
  }
  free(message);
  return 0;
}

int channel_c_stream_is_cancelled(channel_c_stream_emitter_t *emitter) {
  if (!emitter || !emitter->active || !emitter->connection) return 1;
  return emitter->active->cancelled || emitter->connection->closed;
}
