#ifndef CHANNEL_C_INTERNAL_H
#define CHANNEL_C_INTERNAL_H

#include "channel-c/channel_c.h"

typedef struct channel_c_connection channel_c_connection_t;

typedef enum {
  CHANNEL_C_TRANSPORT_REMOTE = 0,
  CHANNEL_C_TRANSPORT_PROCESS = 1,
} channel_c_transport_kind_t;

typedef struct {
  int (*send_text)(void *ctx, const char *text);
  void (*close)(void *ctx);
} channel_c_transport_ops_t;

typedef int (*channel_c_poll_fn)(void *ctx);

channel_c_connection_t *channel_c_connection_create(
  channel_c_channel_t *channel,
  void *state,
  channel_c_state_destroy_fn state_destroy,
  void *state_destroy_user_data,
  void *transport_ctx,
  channel_c_transport_kind_t transport_kind,
  channel_c_transport_ops_t transport_ops
);

void channel_c_connection_retain(channel_c_connection_t *connection);
void channel_c_connection_release(channel_c_connection_t *connection);
void channel_c_connection_mark_connected(channel_c_connection_t *connection, int connected);
void channel_c_connection_receive_text(channel_c_connection_t *connection, const char *text);
void channel_c_connection_on_close(channel_c_connection_t *connection);
void channel_c_connection_stop(channel_c_connection_t *connection);
channel_c_sender_t *channel_c_connection_sender(channel_c_connection_t *connection);
void *channel_c_connection_state(channel_c_connection_t *connection);
int channel_c_connection_is_connected(channel_c_connection_t *connection);
int channel_c_connection_is_closed(channel_c_connection_t *connection);
int channel_c_connection_is_remote_subscribed(channel_c_connection_t *connection, const char *topic);

void channel_c_internal_register_pump(void *ctx, channel_c_poll_fn poll_fn);
void channel_c_internal_unregister_pump(void *ctx);
int channel_c_internal_pump_all(void);
void channel_c_internal_sleep_short(void);

void channel_c_internal_failpoint_reset(void);
void channel_c_internal_failpoint_set(const char *name, int count);
int channel_c_internal_failpoint_hit(const char *name);

channel_c_connection_t *channel_c_internal_client_connection(channel_c_client_t *client);
channel_c_connection_t *channel_c_internal_server_first_connection(channel_c_server_t *server);
channel_c_connection_t *channel_c_internal_process_connection(channel_c_process_t *process);
int channel_c_internal_process_send(channel_c_process_t *process, const char *text);

#endif
