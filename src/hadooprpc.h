
#ifndef HADOOPRPC_H
#define HADOOPRPC_H

#include <google/protobuf-c/protobuf-c.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

#include "proto/datatransfer.pb-c.h"

struct connection_state {
  int sockfd;
  int32_t next_call_id;
  struct sockaddr_in servaddr;
  bool isconnected;
  pthread_mutex_t mutex;
};

struct namenode_state {
  struct connection_state connection;
  const char * clientname;
  pthread_t worker;
  uint32_t packetsize;
  uint64_t blocksize;
  uint32_t replication;
  uint32_t bytesperchecksum;
  Hadoop__Hdfs__ChecksumTypeProto checksumtype;
};

int hadoop_rpc_connect_namenode(
  struct namenode_state * state,
  const char * host,
  const uint16_t port);

int hadoop_rpc_connect_datanode(
  struct connection_state * state,
  const char * host,
  const uint16_t port);

int hadoop_rpc_disconnect(
  struct connection_state * state);

int hadoop_rpc_call_namenode(
  struct namenode_state * state,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out);

int hadoop_rpc_call_datanode(
  struct connection_state * state,
  uint8_t type,
  const ProtobufCMessage * in,
  Hadoop__Hdfs__BlockOpResponseProto ** out);

int hadoop_rpc_receive_packets(
  struct connection_state * state,
  uint64_t skipbytes,
  size_t len,
  uint8_t * to);

int hadoop_rpc_send_packets(
  struct connection_state * state,
  uint8_t * from,
  size_t len,
  off_t offset,
  uint32_t packetsize,
  const Hadoop__Hdfs__ChecksumProto * checksum);

#endif