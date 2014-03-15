

/*
   FUSE: Filesystem in Userspace
   Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

   This program can be distributed under the terms of the GNU GPL.
   See the file COPYING.

   gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#ifdef DEBUG
#include <execinfo.h>
#endif

#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "hadooprpc.h"

#ifdef DEBUG
static void dump_trace() {
  void * buffer[255];
  const int calls = backtrace(buffer,
                              sizeof(buffer) / sizeof(void *));
  backtrace_symbols_fd(buffer, calls, 1);
  exit(EXIT_FAILURE);
}

#endif

static const char *hello_path = "/hello";

#define CALL_NN(method) \
  hadoop_rpc_call( \
    (struct connection_state *) fuse_get_context()->private_data, \
    (const ProtobufCServiceDescriptor *) &hadoop__hdfs__client_namenode_protocol__descriptor, \
    method, \
    (const ProtobufCMessage *) &request, \
    (ProtobufCMessage **) &response);

static int hello_getattr(const char *path, struct stat *stbuf)
{
  int res;
  Hadoop__Hdfs__GetFileInfoRequestProto request = HADOOP__HDFS__GET_FILE_INFO_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFileInfoResponseProto *response = NULL;

  memset(stbuf, 0, sizeof(struct stat));

  request.src = (char *) path;
  res = CALL_NN("getFileInfo");
  if(res < 0)
  {
    return -ENOENT;
  }
  else
  {
    Hadoop__Hdfs__HdfsFileStatusProto * fs = response->fs;

    stbuf->st_size = fs->length;
    switch(fs->filetype)
    {
    case HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_DIR:
    {
      stbuf->st_mode = S_IFDIR;
      break;
    }
    case HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_FILE:
    {
      stbuf->st_mode = S_IFREG;
      break;
    }
    case HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_SYMLINK:
    {
      stbuf->st_mode = S_IFLNK;
      break;
    }
    }
    if(fs->has_blocksize)
    {
      stbuf->st_blksize = fs->blocksize;
    }
    stbuf->st_mtime = fs->modification_time;
    stbuf->st_atime = fs->access_time;

    hadoop__hdfs__get_file_info_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
  (void) offset;
  (void) fi;

  if (strcmp(path, "/") != 0)
    return -ENOENT;

  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);
  filler(buf, hello_path + 1, NULL, 0);

  return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
  if (strcmp(path, hello_path) != 0)
    return -ENOENT;

  if ((fi->flags & 3) != O_RDONLY)
    return -EACCES;

  return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
  (void) buf;
  (void) size;
  (void) offset;
  (void) fi;

  if(strcmp(path, hello_path) != 0)
    return -ENOENT;

  return 0;
}

static struct fuse_operations hello_oper = {
  .getattr  = hello_getattr,
  .readdir  = hello_readdir,
  .open   = hello_open,
  .read   = hello_read,
};

int main(int argc, char *argv[])
{
  struct connection_state state;
  uint16_t port;
  int result;

  if (argc < 3)
  {
    fprintf(stderr,"need namenode host & namenode port as first two argumnets\n");
    return 1;
  }

#ifdef DEBUG
  signal(SIGSEGV, dump_trace);
#endif

  port = atoi(argv[2]);
  memset(&state, 0, sizeof(state));
  result = hadoop_rpc_connect(&state, argv[1], port);
  if(result < 0)
  {
    fprintf(stderr,"connection to hdfs://%s:%d failed: %s\n", argv[1], port, strerror(-result));
    return 1;
  }

  for(int i = 3; i < argc; ++i)
  {
    argv[i - 2] = argv[i];
  }
  return fuse_main(argc - 2, argv, &hello_oper, &state);
}

