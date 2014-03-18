
#ifndef HADOOPRPC_H
#define HADOOPRPC_H

#include <google/protobuf-c/protobuf-c.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>

#include "proto/datatransfer.pb-c.h"

struct connection_state {
  int sockfd;
  int32_t next_call_id;
  struct sockaddr_in servaddr;
  bool isconnected;
};

int hadoop_rpc_connect_namenode(
  struct connection_state * state,
  const char * host,
  const uint16_t port);

int hadoop_rpc_connect_datanode(
  struct connection_state * state,
  const char * host,
  const uint16_t port);

int hadoop_rpc_disconnect(
  struct connection_state * state);

int hadoop_rpc_call_namenode(
  struct connection_state * state,
  const ProtobufCServiceDescriptor * service,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out);

int hadoop_rpc_call_datanode(
  struct connection_state * state,
  uint8_t type,
  const ProtobufCMessage * in,
  Hadoop__Hdfs__BlockOpResponseProto ** out);

int hadoop_rpc_copy_packets(
  struct connection_state * state,
  uint8_t * to);

#endif