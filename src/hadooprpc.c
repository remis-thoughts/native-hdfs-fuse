
#include "hadooprpc.h"
#include "varint.h"
#include "minmax.h"

#include "proto/IpcConnectionContext.pb-c.h"
#include "proto/ProtobufRpcEngine.pb-c.h"
#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "proto/RpcHeader.pb-c.h"
#include "proto/datatransfer.pb-c.h"

#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef NDEBUG
#include <syslog.h>
#endif

// DECLARATIONS ----------------------------------------------

int hadoop_rpc_disconnect(struct connection_state * state);

static
int hadoop_rpc_disconnect_namenode(struct namenode_state * state);

// -----------------------------------------------------------

static
void * hadoop_namenode_worker(void * p)
{
  Hadoop__Hdfs__RenewLeaseRequestProto request = HADOOP__HDFS__RENEW_LEASE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__RenewLeaseResponseProto * response = NULL;
  int res;
  struct namenode_state * state = (struct namenode_state *) p;
  request.clientname = (char *) state->clientname;

  while(true)
  {
    // "If the client is not actively using the lease, it will time out after one minute (default value)."
    // http://itm-vm.shidler.hawaii.edu/HDFS/ArchDocDecomposition.html
    sleep(30);

    res = hadoop_rpc_call_namenode(
      state,
      "renewLease",
      (const ProtobufCMessage *) &request,
      (ProtobufCMessage **) &response);
    if(res == 0)
    {
      hadoop__hdfs__renew_lease_response_proto__free_unpacked(response, NULL);
    }
  }

  return NULL;
}

static inline
ssize_t hadoop_rpc_send(const struct connection_state * state, const void * const it, const ssize_t len)
{
  return sendto(state->sockfd, it, len, 0, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
}

static inline
ssize_t hadoop_rpc_send_int32(const struct connection_state * state, const uint32_t it)
{
  uint32_t it_in_n = htonl(it);
  return sendto(state->sockfd, &it_in_n, sizeof(it_in_n), 0, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
}

static inline
ssize_t hadoop_rpc_send_int16(const struct connection_state * state, const uint16_t it)
{
  uint16_t it_in_n = htons(it);
  return sendto(state->sockfd, &it_in_n, sizeof(it_in_n), 0, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
}

#define CONNECTION_CONTEXT_CALL_ID -3

#define PACK(len, buf, msg) \
  do \
  { \
    uint8_t lenbuf[10]; \
    uint8_t lenlen; \
    len = protobuf_c_message_get_packed_size((const ProtobufCMessage *) msg); \
    lenlen = encode_unsigned_varint(lenbuf, len); \
    buf = alloca(lenlen + len); \
    memcpy(buf, lenbuf, lenlen); \
    protobuf_c_message_pack((const ProtobufCMessage *) msg, buf + lenlen); \
    len += lenlen; \
  } while(0)

static inline
int hadoop_rpc_call_namenode_withlock(
  struct namenode_state * state,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out)
{
  int res;
  void * headerbuf;
  uint32_t headerlen;
  void * requestbuf;
  uint32_t requestlen;
  Hadoop__Common__RpcRequestHeaderProto header = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__INIT;
  Hadoop__Common__RequestHeaderProto request = HADOOP__COMMON__REQUEST_HEADER_PROTO__INIT;
  uint32_t responselen;
  void * responsebuf;
  uint32_t responseheaderlen;
  uint8_t lenlen;
  Hadoop__Common__RpcResponseHeaderProto * response;
  void * msgbuf;
  uint32_t msglen;
  const ProtobufCMethodDescriptor * method = protobuf_c_service_descriptor_get_method_by_name(
    &hadoop__hdfs__client_namenode_protocol__descriptor,
    methodname);
  struct connection_state * connection = (struct connection_state *) state;  // struct first element

  PACK(msglen, msgbuf, in);

  header.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  header.has_rpckind = true;
  header.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  header.has_rpcop = true;
  header.callid = connection->next_call_id++;
  PACK(headerlen, headerbuf, &header);

  request.declaringclassprotocolname = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  request.clientprotocolversion = 1;
  request.methodname = (char *) methodname;
  PACK(requestlen, requestbuf, &request);

  hadoop_rpc_send_int32(connection, headerlen + requestlen + msglen);
  hadoop_rpc_send(connection, headerbuf, headerlen);
  hadoop_rpc_send(connection, requestbuf, requestlen);
  hadoop_rpc_send(connection, msgbuf, msglen);

  res = recvfrom(connection->sockfd, &responselen, sizeof(responselen), MSG_WAITALL, NULL, NULL);
  if(res < 0)
  {
    goto cleanup;
  }
  responselen = ntohl(responselen);
  responsebuf = alloca(responselen);
  res = recvfrom(connection->sockfd, responsebuf, responselen, MSG_WAITALL, NULL, NULL);
  if(res < 0)
  {
    goto cleanup;
  }

  responseheaderlen = decode_unsigned_varint(responsebuf, &lenlen);
  responsebuf += lenlen;
  response = hadoop__common__rpc_response_header_proto__unpack(NULL, responseheaderlen, responsebuf);
  responsebuf += responseheaderlen;

  switch(response->status)
  {
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__SUCCESS:
    hadoop__common__rpc_response_header_proto__free_unpacked(response, NULL);

    responseheaderlen = decode_unsigned_varint(responsebuf, &lenlen);
    responsebuf += lenlen;
    *out = protobuf_c_message_unpack(method->output, NULL, responseheaderlen, responsebuf);

    res = 0;
    goto cleanup;
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__FATAL:
    hadoop_rpc_disconnect_namenode(state);
  // fall-through
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__ERROR:
  default:

    if(response->has_errordetail)
    {
      switch(response->errordetail)
      {
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__FATAL_UNAUTHORIZED:
        res = -EACCES;
        break;
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__FATAL_VERSION_MISMATCH:
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__ERROR_RPC_VERSION_MISMATCH:
        res = -ERPCMISMATCH;
        break;
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__FATAL_INVALID_RPC_HEADER:
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__ERROR_RPC_SERVER:
        res = -EBADRPC;
        break;
      default:
        res = -EINVAL;
        break;
      }
    }
    else
    {
      res = -EINVAL;
    }

    hadoop__common__rpc_response_header_proto__free_unpacked(response, NULL);
    goto cleanup;
  }

cleanup:
  return res;
}

int
hadoop_rpc_call_namenode(
  struct namenode_state * state,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out)
{
  int res;
  struct connection_state * connection = (struct connection_state *) state;  // struct first element

  pthread_mutex_lock(&connection->mutex);
  res = hadoop_rpc_call_namenode_withlock(state, methodname, in, out);
  pthread_mutex_unlock(&connection->mutex);
  return res;
}

int hadoop_rpc_disconnect(struct connection_state * state)
{
  // don't care if we fail, but shutdown instead of close to make
  // sure we've flushed anything we've sent.
  shutdown(state->sockfd, SHUT_WR);
  state->isconnected = false;
  return 0;
}

static
int hadoop_rpc_disconnect_namenode(struct namenode_state * state)
{
  hadoop_rpc_disconnect((struct connection_state *) state);
  pthread_cancel(state->worker); // don't care if we fail
  return 0;
}

int
hadoop_rpc_connect_namenode(struct namenode_state * state, const char * host, const uint16_t port)
{
  int error;
  uint8_t header[] = { 'h', 'r', 'p', 'c', 9, 0, 0};
  Hadoop__Common__IpcConnectionContextProto context = HADOOP__COMMON__IPC_CONNECTION_CONTEXT_PROTO__INIT;
  Hadoop__Common__UserInformationProto userinfo = HADOOP__COMMON__USER_INFORMATION_PROTO__INIT;
  void * contextbuf;
  uint32_t contextlen;
  void * rpcheaderbuf;
  uint32_t rpcheaderlen;
  Hadoop__Common__RpcRequestHeaderProto rpcheader = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__INIT;
  Hadoop__Hdfs__GetServerDefaultsRequestProto defaults_request = HADOOP__HDFS__GET_SERVER_DEFAULTS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetServerDefaultsResponseProto * defaults_response = NULL;
  struct connection_state * connection = (struct connection_state *) state;  // struct first element

  pthread_mutex_lock(&connection->mutex);

  connection->sockfd = socket(AF_INET, SOCK_STREAM, 0);
  connection->servaddr.sin_family = AF_INET;
  connection->servaddr.sin_addr.s_addr = inet_addr(host);
  connection->servaddr.sin_port = htons(port);

  error = connect(connection->sockfd, (struct sockaddr *) &connection->servaddr, sizeof(connection->servaddr));
  if(error < 0)
  {
    goto fail;
  }

  error = hadoop_rpc_send(connection, &header, sizeof(header));
  if(error < 0)
  {
    goto fail;
  }

  userinfo.effectiveuser = getenv("USER");
  context.userinfo = &userinfo;
  context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  PACK(contextlen, contextbuf, &context);

  rpcheader.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  rpcheader.has_rpckind = true;
  rpcheader.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  rpcheader.has_rpcop = true;
  rpcheader.callid = CONNECTION_CONTEXT_CALL_ID;
  PACK(rpcheaderlen, rpcheaderbuf, &rpcheader);

  error = hadoop_rpc_send_int32(connection, rpcheaderlen + contextlen);
  if(error < 0)
  {
    goto fail;
  }
  error = hadoop_rpc_send(connection, rpcheaderbuf, rpcheaderlen);
  if(error < 0)
  {
    goto fail;
  }
  error = hadoop_rpc_send(connection, contextbuf, contextlen);
  if(error < 0)
  {
    goto fail;
  }

  error = pthread_create(&state->worker, NULL, hadoop_namenode_worker, state);
  if(error < 0)
  {
    goto fail;
  }

  error = hadoop_rpc_call_namenode_withlock(
    state,
    "getServerDefaults",
    (const ProtobufCMessage *) &defaults_request,
    (ProtobufCMessage **) &defaults_response);
  if(error < 0)
  {
    goto fail;
  }
  state->packetsize = defaults_response->serverdefaults->writepacketsize;
  state->blocksize = defaults_response->serverdefaults->blocksize;
  state->replication = defaults_response->serverdefaults->replication;
  state->bytesperchecksum = defaults_response->serverdefaults->bytesperchecksum;
  if(defaults_response->serverdefaults->has_checksumtype)
  {
    state->checksumtype = defaults_response->serverdefaults->checksumtype;
  }
  else
  {
    state->checksumtype = HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL; // is this a sensible default?
  }
  hadoop__hdfs__get_server_defaults_response_proto__free_unpacked(defaults_response, NULL);

  connection->isconnected = true;
  pthread_mutex_unlock(&connection->mutex);
  return 0;

fail:
  pthread_mutex_unlock(&connection->mutex);
  hadoop_rpc_disconnect_namenode(state);
  return error;
}

int
hadoop_rpc_connect_datanode(struct connection_state * state, const char * host, const uint16_t port)
{
  int res;

  state->sockfd = socket(AF_INET, SOCK_STREAM, 0);
  state->servaddr.sin_family = AF_INET;
  state->servaddr.sin_addr.s_addr = inet_addr(host);
  state->servaddr.sin_port = htons(port);

  res = connect(state->sockfd, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_rpc_connect_datanode, to %s:%u => %zd",
    host,
    port,
    res);
#endif

  state->isconnected = res == 0;
  return res;
}

static
int hadoop_rpc_receive_proto(
  struct connection_state * state,
  ProtobufCMessage ** out,
  ProtobufCMessage * (* unpack)(ProtobufCAllocator  *, size_t, const uint8_t *))
{
  int res;
  uint8_t len_varint[5];
  uint8_t len_varintlen;
  uint64_t len;
  uint8_t bytesinwrongbuf;
  void * buf;

  res = recvfrom(state->sockfd, &len_varint[0], sizeof(len_varint), MSG_WAITALL, NULL, NULL);
  if(res < 0)
  {
    return res;
  }
  len = decode_unsigned_varint(&len_varint[0], &len_varintlen);
  bytesinwrongbuf = 5 - len_varintlen;
  buf = alloca(len);
  if(bytesinwrongbuf > 0)
  {
    // copy remaining bytes from buffer
    memcpy(buf, len_varint + len_varintlen, bytesinwrongbuf);
  }
  res = recvfrom(state->sockfd, buf + bytesinwrongbuf, len - bytesinwrongbuf, MSG_WAITALL, NULL, NULL);
  if(res < 0)
  {
    return res;
  }
  *out = unpack(NULL, len, buf);
  return 0;
}

int hadoop_rpc_call_datanode(
  struct connection_state * state,
  uint8_t type,
  const ProtobufCMessage * in,
  Hadoop__Hdfs__BlockOpResponseProto ** out)
{
  int res;
  void * buf;
  uint32_t len;
  uint8_t header[] = { 0, 28, type };

  res = hadoop_rpc_send(state, &header, sizeof(header));
  if(res < 0)
  {
    return -EPROTO;
  }
  PACK(len, buf, in);
  res = hadoop_rpc_send(state, buf, len);
  if(res < 0)
  {
    return -EPROTO;
  }

  res = hadoop_rpc_receive_proto(
    state,
    (ProtobufCMessage **) out,
    (ProtobufCMessage * (*)(ProtobufCAllocator  *, size_t, const uint8_t *))hadoop__hdfs__block_op_response_proto__unpack);
  if(res < 0)
  {
    return res;
  }
  switch((*out)->status)
  {
  case HADOOP__HDFS__STATUS__ERROR:
    res = -EPROTO;
    break;
  case HADOOP__HDFS__STATUS__ERROR_CHECKSUM:
    res = -EBADMSG;
    break;
  case HADOOP__HDFS__STATUS__ERROR_INVALID:
    res = -EINVAL;
    break;
  case HADOOP__HDFS__STATUS__ERROR_EXISTS:
    res = -EEXIST;
    break;
  case HADOOP__HDFS__STATUS__ERROR_ACCESS_TOKEN:
    res = -EACCES;
    break;
  case HADOOP__HDFS__STATUS__ERROR_UNSUPPORTED:
    res = -ENOSYS;
    break;
  case HADOOP__HDFS__STATUS__SUCCESS:
  case HADOOP__HDFS__STATUS__CHECKSUM_OK:
    res = 0;
    break;
  default:
    res = -ENOTSUP;
    break;
  }

  if(res < 0)
  {
    hadoop__hdfs__block_op_response_proto__free_unpacked(*out, NULL);
    *out = NULL;
  }

  return res;
}

int hadoop_rpc_receive_packets(
  struct connection_state * state,
  uint64_t skipbytes, // bytes from first packet to skip due to checksum alignment
  size_t len,
  uint8_t * to)
{
  int res;
  uint32_t packetlen;
  uint16_t headerlen;
  bool more = true;
  Hadoop__Hdfs__ClientReadStatusProto ack = HADOOP__HDFS__CLIENT_READ_STATUS_PROTO__INIT;
  void * ackbuf;
  uint32_t acklen;

  while(more && len > 0)
  {
    void * headerbuf;
    Hadoop__Hdfs__PacketHeaderProto * header;
    size_t available;
    ssize_t read = 0;

    res = recvfrom(state->sockfd, &packetlen, sizeof(packetlen), MSG_WAITALL, NULL, NULL);
    if(res < 0)
    {
      return res;
    }
    packetlen = ntohl(packetlen);

    res = recvfrom(state->sockfd, &headerlen, sizeof(headerlen), MSG_WAITALL, NULL, NULL);
    if(res < 0)
    {
      return res;
    }
    headerlen = ntohs(headerlen);

    headerbuf = alloca(headerlen);
    res = recvfrom(state->sockfd, headerbuf, headerlen, MSG_WAITALL, NULL, NULL);
    if(res < 0)
    {
      return res;
    }
    header = hadoop__hdfs__packet_header_proto__unpack(NULL, headerlen, headerbuf);

    available = header->datalen;
    more = !header->lastpacketinblock;
    hadoop__hdfs__packet_header_proto__free_unpacked(header, NULL);

    while(skipbytes > 0 && available > 0)
    {
      // we need buffer space to put the data in ("len") and data in
      // the packet ("available")
      ssize_t skipped = min(min(available, skipbytes), len);

      res = recvfrom(state->sockfd, to, skipped, MSG_WAITALL, NULL, NULL);
      if(res < 0)
      {
        return res;
      }
      available -= skipped;
      skipbytes -= skipped;
      // we won't increment the "to" pointer as we want to overwrite this "skipped"
      // data with the real data in the next block.
    }

    if(available > 0)
    {
      read = min(available, len);
      res = recvfrom(state->sockfd, to, read, MSG_WAITALL, NULL, NULL);
      if(res < 0)
      {
        return res;
      }
      len -= read;
      to += read;
    }
  }

  // ack transfer
  ack.status = HADOOP__HDFS__STATUS__SUCCESS;
  PACK(acklen, ackbuf, &ack);
  res = hadoop_rpc_send(state, ackbuf, acklen);
  if(res < 0)
  {
    return res;
  }

  return 0;
}

static
int hadoop_rpc_send_packet(
  struct connection_state * state,
  int64_t seqno,
  uint8_t * from, // can only be NULL if len is 0
  size_t len, // bytes from "from" to read
  off_t offset, // offset in packet
  const Hadoop__Hdfs__ChecksumProto * checksum)
{
  // Each packet looks like:
  //   PLEN    HLEN      HEADER     CHECKSUMS  DATA
  //   32-bit  16-bit   <protobuf>  <variable length>
  //
  // PLEN:      Payload length
  //            = length(PLEN) + length(CHECKSUMS) + length(DATA)
  //            This length includes its own encoded length in
  //            the sum for historical reasons.
  //
  // HLEN:      Header length
  //            = length(HEADER)
  //
  // HEADER:    the actual packet header fields, encoded in protobuf
  // CHECKSUMS: the crcs for the data chunk. May be missing if
  //            checksums were not requested
  // DATA       the actual block data

  (void) checksum;
  int res;
  uint32_t packetlen;
  uint16_t headerlen;
  void * headerbuf;
  Hadoop__Hdfs__PacketHeaderProto header = HADOOP__HDFS__PACKET_HEADER_PROTO__INIT;
  Hadoop__Hdfs__PipelineAckProto * ack = NULL;

  header.seqno = seqno;
  header.offsetinblock = offset;
  header.lastpacketinblock = len == 0;
  header.datalen = len;
  headerlen = hadoop__hdfs__packet_header_proto__get_packed_size(&header);
  headerbuf = alloca(headerlen);
  hadoop__hdfs__packet_header_proto__pack(&header, headerbuf);
  packetlen = sizeof(packetlen) + 0 + header.datalen;

  res = hadoop_rpc_send_int32(state, packetlen);
  if(res < 0)
  {
    goto endpacket;
  }
  res = hadoop_rpc_send_int16(state, headerlen);
  if(res < 0)
  {
    goto endpacket;
  }
  res = hadoop_rpc_send(state, headerbuf, headerlen);
  if(res < 0)
  {
    goto endpacket;
  }
  res = hadoop_rpc_send(state, from, header.datalen);
  if(res < 0)
  {
    goto endpacket;
  }

  // now get the ack
  res = hadoop_rpc_receive_proto(
    state,
    (ProtobufCMessage **) &ack,
    (ProtobufCMessage * (*)(ProtobufCAllocator  *, size_t, const uint8_t *))hadoop__hdfs__pipeline_ack_proto__unpack);
  if(res < 0)
  {
    goto endpacket;
  }

  if(ack->seqno != seqno)
  {
    res = -EPROTO;
    goto endpacket;
  }
  if(ack->seqno != header.seqno || ack->n_status == 0)
  {
    res = -EPROTO;
    goto endpacket;
  }

  for(size_t i = 0; i < ack->n_status; ++i)
  {
    switch(ack->status[i])
    {
    case HADOOP__HDFS__STATUS__SUCCESS:
    case HADOOP__HDFS__STATUS__CHECKSUM_OK:
      res = 0;
      break;
    default:
      res = -EINVAL;
      goto endpacket;
    }
  }

endpacket:
#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_rpc_send_packet, %s %llu |data|=%zd |packet|=%u |header|=%u offset=%zd => %d",
    res < 0 ? "FAILED to send" : "sent",
    seqno,
    len,
    packetlen,
    headerlen,
    offset,
    res);
#endif
  if(ack)
  {
    hadoop__hdfs__pipeline_ack_proto__free_unpacked(ack, NULL);
  }
  return res;
}

int hadoop_rpc_send_packets(
  struct connection_state * state,
  uint8_t * from,
  size_t len,
  off_t offset,
  uint32_t packetsize,
  const Hadoop__Hdfs__ChecksumProto * checksum)
{
  int res;
  size_t sent = 0;
  int64_t seqno = 0;
  uint8_t * tosend;

  if(from)
  {
    tosend = from;
  }
  else
  {
    // since we'll use this, we'll make the buffer as big as
    // we could possibly need.
    tosend = alloca(packetsize);
    memset(tosend, 0, packetsize);
  }

  while(sent < len)
  {
    size_t packetlen = len - sent;
    off_t packetoffset = offset + sent;
    uint32_t bytespastboundary = packetoffset % checksum->bytesperchecksum;

    if(bytespastboundary != 0)
    {
      // if we have a "partial" checksum - i.e. we're not starting
      // on a "bytesperchecksum" boundary, we can only have this one
      // (partial) chunk in the packet
      packetlen = checksum->bytesperchecksum - bytespastboundary;
    }
    else if(packetlen > packetsize)
    {
      packetlen = packetsize;
    }

    res = hadoop_rpc_send_packet(
      state,
      seqno++,
      tosend,
      packetlen,
      packetoffset,
      checksum);
    if(res < 0)
    {
      return res;
    }

    sent += packetlen;
    if(from)
    {
      // we only need to walk through a data buffer, not the fixed
      // nu
      tosend += packetlen;
    }
  }

  // sent empty packet to finish
  res = hadoop_rpc_send_packet(
    state,
    seqno++,
    NULL,
    0,
    offset + sent,
    checksum);
  if(res < 0)
  {
    return res;
  }

  return 0;
}

