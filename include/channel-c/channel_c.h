#ifndef CHANNEL_C_CHANNEL_C_H
#define CHANNEL_C_CHANNEL_C_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct channel_c_channel channel_c_channel_t;
typedef struct channel_c_sender channel_c_sender_t;
typedef struct channel_c_client channel_c_client_t;
typedef struct channel_c_server channel_c_server_t;
typedef struct channel_c_process channel_c_process_t;
typedef struct channel_c_values channel_c_values_t;
typedef struct channel_c_subscription_handle channel_c_subscription_handle_t;
typedef struct channel_c_stream_emitter channel_c_stream_emitter_t;

typedef struct {
  const char *path;
  const char *body_json;
  const char *context_json;
  channel_c_sender_t *sender;
  void *state;
  void *user_data;
} channel_c_post_request_t;

typedef int (*channel_c_post_handler_fn)(const channel_c_post_request_t *request, char **out_body_json, char **out_error);

typedef struct {
  const char *path;
  const char *body_json;
  const char *context_json;
  channel_c_sender_t *sender;
  void *state;
  void *user_data;
  channel_c_stream_emitter_t *emitter;
} channel_c_stream_request_t;

typedef void (*channel_c_stream_handler_fn)(const channel_c_stream_request_t *request);

typedef struct {
  const char *name;
  const char *request_body_json;
  channel_c_sender_t *sender;
  void *state;
  void *user_data;
} channel_c_subscription_request_t;

typedef int (*channel_c_subscription_resolver_fn)(
  const channel_c_subscription_request_t *request,
  char **out_topic,
  char **out_body_json,
  char **out_error
);

typedef void (*channel_c_event_callback_fn)(const char *body_json, void *user_data);
typedef void *(*channel_c_state_create_fn)(void *user_data);
typedef void (*channel_c_state_destroy_fn)(void *state, void *user_data);

typedef struct {
  channel_c_state_create_fn create;
  channel_c_state_destroy_fn destroy;
  void *user_data;
} channel_c_state_factory_t;

#define CHANNEL_C_POST_IGNORE_BODY 0x1u
#define CHANNEL_C_POST_IGNORE_CONTEXT 0x2u

channel_c_channel_t *channel_c_channel_new(void);
void channel_c_channel_free(channel_c_channel_t *channel);

int channel_c_channel_post(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_post_handler_fn handler,
  void *user_data,
  unsigned int flags
);

int channel_c_channel_stream(
  channel_c_channel_t *channel,
  const char *path,
  channel_c_stream_handler_fn handler,
  void *user_data
);

int channel_c_channel_subscription(
  channel_c_channel_t *channel,
  const char *name,
  channel_c_subscription_resolver_fn resolver,
  void *user_data
);

int channel_c_channel_merge(channel_c_channel_t *destination, channel_c_channel_t *source, const char *prefix);

int channel_c_channel_event_send(
  channel_c_channel_t *channel,
  const char *subscription_name,
  const char *request_body_json,
  const char *body_override_json,
  char **out_error
);

int channel_c_sender_send(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_body_json,
  char **out_error
);

int channel_c_sender_send_string(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_text,
  char **out_body_text,
  char **out_error
);

channel_c_values_t *channel_c_sender_values(
  channel_c_sender_t *sender,
  const char *path,
  const char *body_json,
  const char *context_json,
  char **out_error
);

int channel_c_values_next(channel_c_values_t *values, char **out_body_json, int *out_done, char **out_error);
void channel_c_values_close(channel_c_values_t *values);

int channel_c_sender_subscribe(
  channel_c_sender_t *sender,
  const char *subscription_name,
  const char *body_json,
  const char *context_json,
  channel_c_event_callback_fn callback,
  void *user_data,
  channel_c_subscription_handle_t **out_handle,
  char **out_error
);

void channel_c_subscription_cancel(channel_c_subscription_handle_t *handle);
void channel_c_sender_stop(channel_c_sender_t *sender);

channel_c_process_t *channel_c_process_new(channel_c_channel_t *channel, void *state, channel_c_state_destroy_fn state_destroy);
channel_c_sender_t *channel_c_process_sender(channel_c_process_t *process);
void channel_c_process_stop(channel_c_process_t *process);
void channel_c_process_free(channel_c_process_t *process);

channel_c_client_t *channel_c_client_connect(
  channel_c_channel_t *channel,
  const char *url,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  char **out_error
);
channel_c_sender_t *channel_c_client_sender(channel_c_client_t *client);
int channel_c_client_send_string(
  channel_c_client_t *client,
  const char *path,
  const char *body_text,
  char **out_body_text,
  char **out_error
);
int channel_c_client_poll(channel_c_client_t *client);
int channel_c_client_is_connected(channel_c_client_t *client);
int channel_c_client_remote_is_subscribed(channel_c_client_t *client, const char *topic);
void channel_c_client_stop(channel_c_client_t *client);
void channel_c_client_free(channel_c_client_t *client);

channel_c_server_t *channel_c_server_listen(
  channel_c_channel_t *channel,
  const char *host,
  int port,
  channel_c_state_factory_t state_factory,
  char **out_error
);

int channel_c_server_poll(channel_c_server_t *server);
int channel_c_server_port(channel_c_server_t *server);
void channel_c_server_stop(channel_c_server_t *server);
void channel_c_server_free(channel_c_server_t *server);
int channel_c_server_connection_count(channel_c_server_t *server);

int channel_c_stream_emit_json(channel_c_stream_emitter_t *emitter, const char *body_json);
int channel_c_stream_is_cancelled(channel_c_stream_emitter_t *emitter);

char *channel_c_strdup(const char *source);
void channel_c_free(void *pointer);

#ifdef __cplusplus
}
#endif

#endif
