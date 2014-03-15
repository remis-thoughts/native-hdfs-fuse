
#ifndef HADOOPRPC_H
#define HADOOPRPC_H

#include <google/protobuf-c/protobuf-c.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>

struct connection_state {
  int sockfd;
  int32_t next_call_id;
  struct sockaddr_in servaddr;
  bool isconnected;
};

int hadoop_rpc_connect(
  struct connection_state * state,
  const char * host,
  const uint16_t port);

int hadoop_rpc_call(
  struct connection_state * state,
  const ProtobufCServiceDescriptor * service,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out);

#endif