#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "hadooprpc.h"


int main(int argc, const char * argv[]) 
{
  struct connection_state state = {0};

  if (argc != 5) 
  {
    fprintf(stderr,"usage: nn-host nn-port src dest\n");
    return 1;
  }

  if(hadoop_rpc_connect(&state, argv[1], atoi(argv[2])) < 0)
  {
    return 2;
  }

  {
    Hadoop__Hdfs__RenameRequestProto msg = HADOOP__HDFS__RENAME_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__RenameResponseProto *response = NULL;

    msg.src = (char *) argv[3];
    msg.dst = (char *) argv[4];
    if(hadoop_rpc_call(
      &state,
      (const ProtobufCServiceDescriptor *) &hadoop__hdfs__client_namenode_protocol__descriptor,
      "rename",
      (const ProtobufCMessage *) &msg,
      (ProtobufCMessage **) &response) < 0)
    {
      return 4;
    }

    if(!response->result)
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

