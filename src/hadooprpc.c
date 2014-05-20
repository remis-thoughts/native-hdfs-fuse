
#include "hadooprpc.h"
#include "varint.h"
#include "minmax.h"
#include "roundup.h"

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

uint32_t crc32c(uint32_t crc, const void * buf, size_t len);

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
        res = -EPROTONOSUPPORT;
        break;
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__FATAL_INVALID_RPC_HEADER:
      case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_ERROR_CODE_PROTO__ERROR_RPC_SERVER:
        res = -EBADMSG;
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
  // don't care if we fail, and we use SO_LINGER to ensure we flush
  // any outgoing data.
  close(state->sockfd);
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

static
int hadoop_rpc_do_connect(struct connection_state * connection, const char * host, const uint16_t port)
{
  int res;
  struct linger so_linger = {
    .l_onoff = true,
    .l_linger = 30
  };

  connection->sockfd = socket(AF_INET, SOCK_STREAM, 0);
  connection->servaddr.sin_family = AF_INET;
  connection->servaddr.sin_addr.s_addr = inet_addr(host);
  connection->servaddr.sin_port = htons(port);

  res = connect(connection->sockfd, (struct sockaddr *) &connection->servaddr, sizeof(connection->servaddr));
  if(res < 0)
  {
    return res;
  }

  res = setsockopt(
    connection->sockfd,
    SOL_SOCKET,
    SO_LINGER,
    &so_linger,
    sizeof(so_linger));
  if(res < 0)
  {
    return res;
  }

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

  error = hadoop_rpc_do_connect(connection, host, port);
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
  int res = hadoop_rpc_do_connect(state, host, port);

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_rpc_connect_datanode, to %s:%u => %d",
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
  struct Hadoop_Fuse_Buffer_Pos * from, // or NULL to send zeros
  size_t len, // bytes from "from" to read
  off_t blockoffset,
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

  int res;
  uint32_t packetlen;
  uint16_t headerlen;
  uint8_t checksumlen; // length of a single checksum
  uint32_t n_checksums;
  void * headerbuf;
  Hadoop__Hdfs__PacketHeaderProto header = HADOOP__HDFS__PACKET_HEADER_PROTO__INIT;
  Hadoop__Hdfs__PipelineAckProto * ack = NULL;

  assert(from);
  assert(len <= from->len - from->bufferoffset); // we must have this much data available

  switch(checksum->type)
  {
  case HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32C:
    checksumlen = sizeof(uint32_t);
    break;
  case HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32:
    // CRC32C is the default, TODO: implement this?
    return -ENOSYS;
  case HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL:
    checksumlen = 0;
    break;
  default:
    return -ENOSYS;
  }

  n_checksums = roundup(len, checksum->bytesperchecksum);
  packetlen = sizeof(packetlen) + n_checksums * checksumlen + len;

  header.seqno = seqno;
  header.offsetinblock = blockoffset;
  header.lastpacketinblock = len == 0;
  header.datalen = len;
  headerlen = hadoop__hdfs__packet_header_proto__get_packed_size(&header);
  headerbuf = alloca(headerlen);
  hadoop__hdfs__packet_header_proto__pack(&header, headerbuf);

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

  if(len > 0)
  {
    void * packet = NULL;

    // we need to assemble the packet if it appears across multiple buffers
    {
      size_t idx = 0;
      size_t written = 0;

      for(uint8_t i = 0; i < from->n_buffers && written < len; ++i)
      {
        const struct Hadoop_Fuse_Buffer * buffer = from->buffers + i;

        if(idx + buffer->len > from->bufferoffset)
        {
          // we're in the bit of data we want to write
          off_t offset = idx > from->bufferoffset ? 0 : (from->bufferoffset - idx); // offset into buffer
          size_t towrite = min(len - written, buffer->len - offset);

          if(towrite == len)
          {
            // we don't need to assemble the packet as it's in a single buffer
            if(buffer->data)
            {
              packet = buffer->data + offset;
            }
            else
            {
              // we don't have a real buffer to read data from, so make one and fill it with nulls
              // TODO: could we pass NULLs to sendto without creating this?
              packet = alloca(len);
              memset(packet, 0, len);
            }
            break;
          }
          else if(towrite > 0)
          {
            // we need a bit from this buffer and a bit from at least one other one.

            if(!packet)
            {
              packet = alloca(len);
            }

            if(buffer->data)
            {
              memcpy(packet + written, buffer->data + offset, towrite);
            }
            else
            {
              memset(packet + written, 0, towrite);
            }

            written += towrite;
          }
        }

        idx += buffer->len;
      }
    }

    if(checksumlen > 0)
    {
      // write out the checksums

      assert(checksum->type == HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32C); // TODO: currently only one implementation

      for(uint32_t i = 0; i < n_checksums; ++i)
      {
        uint32_t packetidx = i * checksum->bytesperchecksum;

        res = hadoop_rpc_send_int32(
          state,
          crc32c(
            0, // we're not computing the crc32c incrementally
            packet + packetidx,
            min(checksum->bytesperchecksum, len - packetidx)));
        if(res < 0)
        {
          goto endpacket;
        }
      }
    }

    res = hadoop_rpc_send(state, packet, len);
    if(res < 0)
    {
      goto endpacket;
    }

    from->bufferoffset += len;
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
    "hadoop_rpc_send_packet, %s seqno=%llu |data|=%zd |packet|=%u |header|=%u offset=%zd with %u checksums => %d",
    res < 0 ? "FAILED to send" : "sent",
    seqno,
    len,
    packetlen,
    headerlen,
    blockoffset,
    n_checksums,
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
  struct Hadoop_Fuse_Buffer_Pos * from,
  uint64_t len,           // bytes to write
  off_t blockoffset,
  uint32_t packetsize,
  const Hadoop__Hdfs__ChecksumProto * checksum)
{
  int res;
  off_t initialbufferoffset = from->bufferoffset;
  int64_t seqno = 0;

  while(true)
  {
    uint64_t bytessent = from->bufferoffset - initialbufferoffset;
    size_t packetlen = min(len - bytessent, packetsize);
    off_t packetoffset = blockoffset + bytessent;
    uint32_t bytespastboundary = packetoffset % checksum->bytesperchecksum;

    if(packetlen > 0 && bytespastboundary != 0)
    {
      // if we have a "partial" checksum - i.e. we're not starting
      // on a "bytesperchecksum" boundary, we can only have this one
      // (partial) chunk in the packet
      packetlen = checksum->bytesperchecksum - bytespastboundary;
    }

    res = hadoop_rpc_send_packet(
      state,
      seqno++,
      from,
      packetlen,
      packetoffset,
      checksum);
    if(res < 0)
    {
      return res;
    }
    if(packetlen == 0)
    {
      break;
    }
  }

  return res < 0 ? res : 0;
}

