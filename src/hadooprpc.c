
#include "hadooprpc.h"
#include "varint.h"

#include "proto/IpcConnectionContext.pb-c.h"
#include "proto/ProtobufRpcEngine.pb-c.h"
#include "proto/RpcHeader.pb-c.h"

#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

int hadoop_rpc_send_out_of_band(
  struct connection_state * state,
  const void * msgbuf,
  const uint32_t msglen,
  const int32_t callid)
{
  void * headerbuf;
  uint32_t headerlen;
  Hadoop__Common__RpcRequestHeaderProto header = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__INIT;

  header.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  header.has_rpckind = true;
  header.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  header.has_rpcop = true;
  header.callid = callid;
  PACK(headerlen, headerbuf, &header);

  if(hadoop_rpc_send_int32(state, headerlen + msglen) < 0
     || hadoop_rpc_send(state, headerbuf, headerlen) < 0
     || hadoop_rpc_send(state, msgbuf, msglen) < 0)
  {
    return -EINVAL;
  }
  else
  {
    return 0;
  }
}

int hadoop_rpc_call(
  struct connection_state * state,
  const ProtobufCServiceDescriptor * service,
  const char * methodname,
  const ProtobufCMessage * in,
  ProtobufCMessage ** out)
{
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
  const ProtobufCMethodDescriptor * method = protobuf_c_service_descriptor_get_method_by_name(service, methodname);

  PACK(msglen, msgbuf, in);

  header.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  header.has_rpckind = true;
  header.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  header.has_rpcop = true;
  header.callid = state->next_call_id++;
  PACK(headerlen, headerbuf, &header);

  request.declaringclassprotocolname = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  request.clientprotocolversion = 1;
  request.methodname = (char *) methodname;
  PACK(requestlen, requestbuf, &request);

  hadoop_rpc_send_int32(state, headerlen + requestlen + msglen);
  hadoop_rpc_send(state, headerbuf, headerlen);
  hadoop_rpc_send(state, requestbuf, requestlen);
  hadoop_rpc_send(state, msgbuf, msglen);

  if(recvfrom(state->sockfd, &responselen, sizeof(responselen), MSG_WAITALL, NULL, NULL) < 0)
  {
    return -1;
  }
  responselen = ntohl(responselen);
  responsebuf = alloca(responselen);
  if(recvfrom(state->sockfd, responsebuf, responselen, MSG_WAITALL, NULL, NULL) < 0)
  {
    return -1;
  }

  responseheaderlen = decode_unsigned_varint(responsebuf, &lenlen);
  responsebuf += lenlen;
  response = hadoop__common__rpc_response_header_proto__unpack(NULL, responseheaderlen, responsebuf);
  responsebuf += responseheaderlen;

  switch(response->status)
  {
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__SUCCESS:
  {
    hadoop__common__rpc_response_header_proto__free_unpacked(response, NULL);

    responseheaderlen = decode_unsigned_varint(responsebuf, &lenlen);
    responsebuf += lenlen;
    *out = protobuf_c_message_unpack(method->output, NULL, responseheaderlen, responsebuf);

    return 0;
  }
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__FATAL:
  {
    state->isconnected = false;
    close(state->sockfd);
    // fall-through
  }
  case HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__ERROR:
  default:
  {
    const char * error;
    if(response->errormsg)
    {
      error = response->errormsg;
    }
    else
    {
      error = "(no msg)";
    }
    fprintf(stderr, "Error! %s\n", error);

    hadoop__common__rpc_response_header_proto__free_unpacked(response, NULL);
    return -1;
  }
  }
}

int hadoop_rpc_connect(struct connection_state * state, const char * host, const uint16_t port)
{
  uint32_t len;
  uint8_t header[] = { 'h', 'r', 'p', 'c', 9, 0, 0};
  Hadoop__Common__IpcConnectionContextProto context = HADOOP__COMMON__IPC_CONNECTION_CONTEXT_PROTO__INIT;
  Hadoop__Common__UserInformationProto userinfo = HADOOP__COMMON__USER_INFORMATION_PROTO__INIT;
  void *buf;
  int error;

  state->sockfd = socket(AF_INET,SOCK_STREAM,0);
  state->servaddr.sin_family = AF_INET;
  state->servaddr.sin_addr.s_addr = inet_addr(host);
  state->servaddr.sin_port = htons(port);

  error = connect(state->sockfd, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
  if(error < 0)
  {
    error = -EHOSTUNREACH;
    goto fail;
  }

  error = hadoop_rpc_send(state, &header, sizeof(header));
  if(error < 0)
  {
    error = -EPROTO;
    goto fail;
  }

  userinfo.effectiveuser = getenv("USER");
  context.userinfo = &userinfo;
  context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  PACK(len, buf, &context);

  error = hadoop_rpc_send_out_of_band(state, buf, len, CONNECTION_CONTEXT_CALL_ID);
  if(error < 0)
  {
    error = -EBADRPC;
    goto fail;
  }

  state->isconnected = true;
  return 0;

fail:
  close(state->sockfd);
  state->isconnected = false;
  return error;
}