#include <stdlib.h>

#include "internal.h"

struct channel_c_process {
  int stopped;
  channel_c_connection_t *connection;
};

static int process_send_text(void *ctx, const char *text) {
  struct channel_c_process *process = (struct channel_c_process *)ctx;
  if (!process || process->stopped || !process->connection || !text) return -1;
  channel_c_connection_receive_text(process->connection, text);
  return 0;
}

static void process_close(void *ctx) {
  struct channel_c_process *process = (struct channel_c_process *)ctx;
  if (!process) return;
  process->stopped = 1;
}

channel_c_process_t *channel_c_process_new(
  channel_c_channel_t *channel,
  void *state,
  channel_c_state_destroy_fn state_destroy
) {
  struct channel_c_process *process;
  channel_c_transport_ops_t transport_ops;

  if (!channel) return NULL;
  if (channel_c_internal_failpoint_hit("process.process_alloc_fail")) return NULL;

  process = (struct channel_c_process *)calloc(1, sizeof(struct channel_c_process));
  if (!process) return NULL;

  transport_ops.send_text = process_send_text;
  transport_ops.close = process_close;

  process->connection = channel_c_internal_failpoint_hit("process.connection_create_fail")
                          ? NULL
                          : channel_c_connection_create(
                              channel,
                              state,
                              state_destroy,
                              NULL,
                              process,
                              CHANNEL_C_TRANSPORT_PROCESS,
                              transport_ops
                            );
  if (!process->connection) {
    free(process);
    return NULL;
  }

  process->stopped = 0;
  channel_c_connection_mark_connected(process->connection, 1);
  return process;
}

channel_c_sender_t *channel_c_process_sender(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return NULL;
  return channel_c_connection_sender(p->connection);
}

void channel_c_process_stop(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return;
  p->stopped = 1;
  channel_c_connection_stop(p->connection);
}

void channel_c_process_free(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  if (!p) return;
  channel_c_process_stop(process);
  channel_c_connection_release(p->connection);
  free(p);
}

channel_c_connection_t *channel_c_internal_process_connection(channel_c_process_t *process) {
  struct channel_c_process *p = (struct channel_c_process *)process;
  return p ? p->connection : NULL;
}

int channel_c_internal_process_send(channel_c_process_t *process, const char *text) {
  return process_send_text((void *)process, text);
}
