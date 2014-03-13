#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "proto/IpcConnectionContext.pb-c.h"
#include "proto/ProtobufRpcEngine.pb-c.h"
#include "proto/RpcHeader.pb-c.h"
#include "varint.h"

typedef struct {
  int sockfd;
  int32_t next_call_id;
  struct sockaddr_in servaddr;
} connection_state;

#define CONNECTION_CONTEXT_CALL_ID -3

#define PACK(len, buf, msg, prefix) \
do \
{ \
  uint8_t lenbuf[10]; \
  uint8_t lenlen; \
  len = prefix ## __get_packed_size(&msg); \
  lenlen = encode_unsigned_varint(lenbuf, len); \
  buf = alloca(lenlen + len); \
  memcpy(buf, lenbuf, lenlen); \
  prefix ## __pack(&msg, buf + lenlen); \
  len += lenlen; \
} while(0)

ssize_t example_send(const connection_state * state, const void * const it, const ssize_t len)
{
  ssize_t bytes = sendto(state->sockfd, it, len, 0, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
  fprintf(stderr,"sendto: %ld of %ld\n", bytes, len);
  return bytes;
}

ssize_t example_send_int32(const connection_state * state, const uint32_t it)
{
  uint32_t it_in_n = htonl(it);
  ssize_t bytes = sendto(state->sockfd, &it_in_n, sizeof(it_in_n), 0, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
  fprintf(stderr,"send int: %ld\n", bytes);
  return bytes;
}

/**
 * Returns the socket descriptor, or -1 on error
 */
int example_connect(connection_state * state, const char * host, const uint16_t port)
{
  int n;
  ssize_t bytes;
  void * contextbuf;
  uint32_t len;
  uint32_t lentosend;
  uint8_t  header[] = { 'h', 'r', 'p', 'c', 9, 0, 0};

  state->sockfd = socket(AF_INET,SOCK_STREAM,0);
  state->servaddr.sin_family = AF_INET;
  state->servaddr.sin_addr.s_addr = inet_addr(host);
  state->servaddr.sin_port = htons(port);

  n = connect(state->sockfd, (struct sockaddr *) &state->servaddr, sizeof(state->servaddr));
  fprintf(stderr,"connect: %d\n", n);
  if(n < 0)
  {
    return -1;
  }
  example_send(state, &header, sizeof(header));
  return 0;
}

int send_rpc_out_of_band(connection_state * state, const void * const msgbuf, const uint32_t msglen, const int32_t callid)
{
  void * headerbuf;
  uint32_t headerlen;
  Hadoop__Common__RpcRequestHeaderProto header = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__INIT;

  header.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  header.has_rpckind = true;
  header.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  header.has_rpcop = true;
  header.callid = callid;
  PACK(headerlen, headerbuf, header, hadoop__common__rpc_request_header_proto);

  example_send_int32(state, headerlen + msglen);
  example_send(state, headerbuf, headerlen);
  example_send(state, msgbuf, msglen);

  return 0;
}

int send_rpc(
  connection_state * const state, 
  const void * const msgbuf, 
  const uint32_t msglen, 
  const char * const methodname,
  void ** const out)
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

  header.rpckind = HADOOP__COMMON__RPC_KIND_PROTO__RPC_PROTOCOL_BUFFER;
  header.has_rpckind = true;
  header.rpcop = HADOOP__COMMON__RPC_REQUEST_HEADER_PROTO__OPERATION_PROTO__RPC_FINAL_PACKET;
  header.has_rpcop = true;
  header.callid = state->next_call_id++;
  PACK(headerlen, headerbuf, header, hadoop__common__rpc_request_header_proto);

  request.declaringclassprotocolname = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  request.clientprotocolversion = 1;
  request.methodname = (char *) methodname;
  PACK(requestlen, requestbuf, request, hadoop__common__request_header_proto);

  example_send_int32(state, headerlen + requestlen + msglen);
  example_send(state, headerbuf, headerlen);
  example_send(state, requestbuf, requestlen);
  example_send(state, msgbuf, msglen);

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

  if(response->status == HADOOP__COMMON__RPC_RESPONSE_HEADER_PROTO__RPC_STATUS_PROTO__SUCCESS)
  {
    fprintf(stderr, "OK.\n");
    hadoop__common__rpc_response_header_proto__free_unpacked(response, NULL);


    responseheaderlen = decode_unsigned_varint(responsebuf, &lenlen);
    responsebuf += lenlen;
    *out = hadoop__hdfs__rename_response_proto__unpack(NULL, responseheaderlen, responsebuf);

    return 0;
  }
  else
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

int main(int argc, const char * argv[]) 
{
  connection_state state = {0};
  int n;
  char recvline[1000];

  if (argc != 5) 
  {
    fprintf(stderr,"usage: nn-host nn-port src dest\n");
    return 1;
  }

  if(example_connect(&state, argv[1], atoi(argv[2])) < 0)
  {
    return 2;
  }

  {
    Hadoop__Common__IpcConnectionContextProto context = HADOOP__COMMON__IPC_CONNECTION_CONTEXT_PROTO__INIT;
    Hadoop__Common__UserInformationProto userinfo = HADOOP__COMMON__USER_INFORMATION_PROTO__INIT;
    void *buf;
    uint32_t len;

    userinfo.effectiveuser = getenv("USER");
    context.userinfo = &userinfo;
    context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

    PACK(len, buf, context, hadoop__common__ipc_connection_context_proto);
    if(send_rpc_out_of_band(&state, buf, len, CONNECTION_CONTEXT_CALL_ID) < 0)
    {
      return 3;
    }
  }

  {
    Hadoop__Hdfs__RenameRequestProto msg = HADOOP__HDFS__RENAME_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__RenameResponseProto *response = NULL;
    void *buf;
    uint32_t len;
    bool result;

    msg.src = (char *) argv[3];
    msg.dst = (char *) argv[4];
    PACK(len, buf, msg, hadoop__hdfs__rename_request_proto);
    if(send_rpc(&state, buf, len, "rename", (void **) &response) < 0)
    {
      return 4;
    }
    result = response->result;
    hadoop__hdfs__rename_response_proto__free_unpacked(response, NULL);

    if(!result)
    {
      fprintf(stderr, "couldn't rename\n");
      return 6;
    }
  }

  if(!close(state.sockfd))
  {
    return 5;
  }
  return 0;
}

