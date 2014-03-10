#include <stdio.h>
#include <stdlib.h>
#include "proto/ClientNamenodeProtocol.pb-c.h"
#include <google/protobuf-c/protobuf-c-rpc.h>


int main (int argc, const char * argv[]) 
{
  Hadoop__Hdfs__RenameRequestProto msg = HADOOP__HDFS__RENAME_REQUEST_PROTO__INIT;
  void *buf;                     // Buffer to store serialized data
  unsigned len;                  // Length of serialized data

  if (argc != 3)
  {
    fprintf(stderr,"usage: src dest\n");
    return 1;
  }

  msg.src = argv[1];
  msg.dst = argv[2];
  len = hadoop__hdfs__rename_request_proto__get_packed_size(&msg);

  buf = malloc(len);
  hadoop__hdfs__rename_request_proto__pack(&msg,buf);

  fprintf(stderr,"Writing %d serialized bytes\n",len);
  fwrite(buf,len,1,stdout);

  free(buf);
  return 0;
}

