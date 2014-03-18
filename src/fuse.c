

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
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#ifndef NDEBUG
#include <execinfo.h>
#include <assert.h>
#endif

#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "proto/datatransfer.pb-c.h"
#include "hadooprpc.h"

#ifndef NDEBUG
static void dump_trace() {
  void * buffer[255];
  const int calls = backtrace(buffer,
                              sizeof(buffer) / sizeof(void *));
  backtrace_symbols_fd(buffer, calls, 1);
  exit(EXIT_FAILURE);
}

#endif

#ifndef min
#define min(a, b) (((a) < (b)) ? (a) : (b))
#define max(a, b) (((a) > (b)) ? (a) : (b))
#endif

#define CALL_NN(method, request, response) \
  hadoop_rpc_call_namenode( \
    (struct connection_state *) fuse_get_context()->private_data, \
    (const ProtobufCServiceDescriptor *) &hadoop__hdfs__client_namenode_protocol__descriptor, \
    method, \
    (const ProtobufCMessage *) &request, \
    (ProtobufCMessage **) &response);

static
void unpack_filestatus(Hadoop__Hdfs__HdfsFileStatusProto * fs, struct stat *stbuf)
{
  struct passwd * owner;
  struct group * group;

  assert(fs);
  assert(stbuf);

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
  if(fs->permission)
  {
    stbuf->st_mode |= fs->permission->perm;
  }
  if(fs->has_blocksize)
  {
    stbuf->st_blksize = fs->blocksize;
  }
  stbuf->st_mtime = fs->modification_time;
  stbuf->st_atime = fs->access_time;

  owner = getpwnam(fs->owner);
  if(owner)
  {
    stbuf->st_uid = owner->pw_uid;
  }
  group = getgrnam(fs->group);
  if(group)
  {
    stbuf->st_gid = group->gr_gid;
  }
}

static
int hadoop_fuse_getattr(const char *path, struct stat *stbuf)
{
  int res;
  Hadoop__Hdfs__GetFileInfoRequestProto request = HADOOP__HDFS__GET_FILE_INFO_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFileInfoResponseProto *response = NULL;

  memset(stbuf, 0, sizeof(struct stat));

  request.src = (char *) path;
  res = CALL_NN("getFileInfo", request, response);
  if(res < 0)
  {
    return res;
  }
  else if(!response->fs)
  {
    hadoop__hdfs__get_file_info_response_proto__free_unpacked(response, NULL);
    return -ENOENT;
  }
  else
  {
    unpack_filestatus(response->fs, stbuf);
    hadoop__hdfs__get_file_info_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_readlink(const char * path, char * buf, size_t len)
{
  int res;
  Hadoop__Hdfs__GetLinkTargetRequestProto request = HADOOP__HDFS__GET_LINK_TARGET_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetLinkTargetResponseProto *response = NULL;

  request.path = (char *) path;
  res = CALL_NN("getLinkTarget", request, response);
  if(res < 0)
  {
    return res;
  }
  else if (!response->targetpath)
  {
    hadoop__hdfs__get_link_target_response_proto__free_unpacked(response, NULL);
    return -ENOENT;
  }
  else
  {
    strncpy(buf, response->targetpath, len);
    buf[len - 1] = '\0';
    hadoop__hdfs__get_link_target_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_utimens(const char * src, const struct timespec tv[2])
{
  int res;
  Hadoop__Hdfs__SetTimesRequestProto request = HADOOP__HDFS__SET_TIMES_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__SetTimesResponseProto *response = NULL;

  request.src = (char *) src;
  request.atime = tv[0].tv_sec * 1000 + tv[0].tv_nsec / 1000000;
  request.mtime = tv[1].tv_sec * 1000 + tv[1].tv_nsec / 1000000;
  res = CALL_NN("setTimes", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    hadoop__hdfs__set_times_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_chown(const char * src, uid_t uid, gid_t gid)
{
  int res;
  struct passwd * owner;
  struct group * group;
  Hadoop__Hdfs__SetOwnerRequestProto request = HADOOP__HDFS__SET_OWNER_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__SetOwnerResponseProto *response = NULL;

  request.src = (char *) src;
  owner = getpwuid(uid);
  if(owner)
  {
    request.username = owner->pw_name;
  }
  group = getgrgid(gid);
  if(group)
  {
    request.groupname = group->gr_name;
  }

  res = CALL_NN("setOwner", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    hadoop__hdfs__set_owner_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_symlink(const char * target, const char * link)
{
  int res;
  Hadoop__Hdfs__CreateSymlinkRequestProto request = HADOOP__HDFS__CREATE_SYMLINK_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__FsPermissionProto permission = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
  Hadoop__Hdfs__CreateSymlinkResponseProto *response = NULL;

  request.link = (char *) link;
  request.target = (char *) target;
  request.dirperm = &permission;
  request.createparent = false;
  permission.perm = 0777; // don't actually matter as we never create parents
  res = CALL_NN("createSymlink", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    hadoop__hdfs__create_symlink_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_rename(const char * src, const char * dst)
{
  int res;
  Hadoop__Hdfs__RenameRequestProto request = HADOOP__HDFS__RENAME_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__RenameResponseProto *response = NULL;

  request.src = (char *) src;
  request.dst = (char *) dst;
  res = CALL_NN("rename", request, response);
  if(res < 0)
  {
    return res;
  }
  else if(!response->result)
  {
    hadoop__hdfs__rename_response_proto__free_unpacked(response, NULL);
    return -EINVAL;
  }
  else
  {
    hadoop__hdfs__rename_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_chmod(const char * src, mode_t perm)
{
  int res;
  Hadoop__Hdfs__SetPermissionRequestProto request = HADOOP__HDFS__SET_PERMISSION_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__FsPermissionProto permission = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
  Hadoop__Hdfs__SetPermissionResponseProto *response = NULL;

  request.src = (char *) src;
  request.permission = &permission;
  permission.perm = perm;
  res = CALL_NN("setPermission", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    hadoop__hdfs__set_permission_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_statfs(const char * src, struct statvfs * stat)
{
  int res;
  uint64_t block_size;
  (void) src; // HDFS only supports getPreferredBlockSize for files
  Hadoop__Hdfs__GetFsStatusRequestProto fs_request = HADOOP__HDFS__GET_FS_STATUS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetServerDefaultsRequestProto block_request = HADOOP__HDFS__GET_SERVER_DEFAULTS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFsStatsResponseProto * fs_response = NULL;
  Hadoop__Hdfs__GetServerDefaultsResponseProto * block_response = NULL;

  res = CALL_NN("getFsStats", fs_request, fs_response);
  if(res < 0)
  {
    return res;
  }
  res = CALL_NN("getServerDefaults", block_request, block_response);
  if(res < 0)
  {
    return res;
  }

  stat->f_bsize = stat->f_frsize = block_size = block_response->serverdefaults->blocksize;
  stat->f_blocks = fs_response->capacity / block_size;
  stat->f_bfree = stat->f_bavail = fs_response->remaining / block_size;
  stat->f_namemax = 0xFFFFFFFF; // java String max length

  hadoop__hdfs__get_fs_stats_response_proto__free_unpacked(fs_response, NULL);
  hadoop__hdfs__get_server_defaults_response_proto__free_unpacked(block_response, NULL);
  return 0;
}

static
int hadoop_fuse_delete(const char * src)
{
  int res;
  Hadoop__Hdfs__DeleteRequestProto request = HADOOP__HDFS__DELETE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__DeleteResponseProto *response = NULL;

  request.src = (char *) src;
  request.recursive = false;
  res = CALL_NN("delete", request, response);
  if(res < 0)
  {
    return res;
  }
  else if(!response->result)
  {
    hadoop__hdfs__delete_response_proto__free_unpacked(response, NULL);
    return -EINVAL;
  }
  else
  {
    hadoop__hdfs__delete_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_mkdir(const char * src, mode_t perm)
{
  int res;
  Hadoop__Hdfs__MkdirsRequestProto request = HADOOP__HDFS__MKDIRS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__FsPermissionProto permission = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
  Hadoop__Hdfs__MkdirsResponseProto *response = NULL;

  request.src = (char *) src;
  request.masked = &permission;
  request.createparent = false;
  permission.perm = perm;
  res = CALL_NN("mkdirs", request, response);
  if(res < 0)
  {
    return res;
  }
  else if(!response->result)
  {
    hadoop__hdfs__mkdirs_response_proto__free_unpacked(response, NULL);
    return -EINVAL;
  }
  else
  {
    hadoop__hdfs__mkdirs_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
  (void) offset;
  (void) fi;
  int res;
  Hadoop__Hdfs__GetListingRequestProto request = HADOOP__HDFS__GET_LISTING_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetListingResponseProto *response = NULL;

  request.src = (char *) path;
  request.startafter.len = 0;
  request.startafter.data = NULL;
  request.needlocation = false;

  res = CALL_NN("getListing", request, response);
  if(res < 0)
  {
    return res;
  }

  for(int i = 0, max = response->dirlist->n_partiallisting; i < max; ++i)
  {
    struct stat stbuf;
    Hadoop__Hdfs__HdfsFileStatusProto * file = response->dirlist->partiallisting[i];
    struct stat * forfuse;
    char * filename;

    if(file)
    {
      memset(&stbuf, 0, sizeof(struct stat));
      unpack_filestatus(file, &stbuf);
      forfuse = &stbuf;
    }
    else
    {
      forfuse = NULL;
    }

    filename = malloc(file->path.len);
    if(!filename)
    {
      return -ENOMEM;
    }
    strncpy(filename, (const char *) file->path.data, file->path.len);
    int filled = filler(buf, filename, forfuse, 0);
    if(filled != 0)
    {
      return -ENOMEM;
    }
  }

  hadoop__hdfs__get_listing_response_proto__free_unpacked(response, NULL);
  return 0;
}

static
int hadoop_fuse_open(const char * path, struct fuse_file_info * fi)
{
  (void) path;

  if ((fi->flags & 3) == O_RDONLY)
  {
    // HDFS doesn't need to lock to read a file
    return 0;
  }
  else
  {
    return -EROFS;
  }
}

static
int hadoop_fuse_read(const char * src, char * buf, size_t size, off_t offset, struct fuse_file_info * fi)
{
  (void) buf;
  (void) fi;
  int res;
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto *response = NULL;

  request.src = (char *) src;
  request.offset = offset;
  request.length = size;
  res = CALL_NN("getBlockLocations", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    Hadoop__Hdfs__LocatedBlocksProto * locations = response->locations;

    if(!locations)
    {
      res = -ENOENT;
    }
    else if(locations->underconstruction)
    {
      res = -EBUSY;
    }
    else
    {
      Hadoop__Hdfs__ClientOperationHeaderProto clientheader = HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
      Hadoop__Hdfs__BaseHeaderProto baseheader = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;

      clientheader.clientname = "hadoop_fuse";

      for (size_t b = 0; b < locations->n_blocks; ++b)
      {
        Hadoop__Hdfs__OpReadBlockProto op = HADOOP__HDFS__OP_READ_BLOCK_PROTO__INIT;
        Hadoop__Hdfs__LocatedBlockProto * block = locations->blocks[b];

        if(block->corrupt)
        {
          res = -EBADMSG;
        }
        else
        {
          Hadoop__Hdfs__BlockOpResponseProto *opresponse = NULL;

          baseheader.block = block->b;
          clientheader.baseheader = &baseheader;
          op.header = &clientheader;
          op.has_sendchecksums = true;
          op.sendchecksums = false;
          op.offset = min(offset - block->offset, 0);
          op.len = min(block->b->numbytes - op.offset, size);

          // for now, don't care about which locatoin we get it from
          for (size_t l = 0; l < block->n_locs; ++l)
          {
            Hadoop__Hdfs__DatanodeInfoProto * location = block->locs[l];
            struct connection_state dn_state;

            memset(&dn_state, 0, sizeof(dn_state));
            res = hadoop_rpc_connect_datanode(&dn_state, location->id->ipaddr, location->id->xferport);
            if(res < 0)
            {
              continue;
            }

            res = hadoop_rpc_call_datanode(&dn_state, 81, (const ProtobufCMessage *) &op, &opresponse);
            if(res < 0)
            {
              hadoop_rpc_disconnect(&dn_state);
              continue;
            }
            hadoop__hdfs__block_op_response_proto__free_unpacked(opresponse, NULL);

            res = hadoop_rpc_copy_packets(
              &dn_state,
              (uint8_t *) buf);
            if(res < 0)
            {
              hadoop_rpc_disconnect(&dn_state);
              continue;
            }

            hadoop_rpc_disconnect(&dn_state);
            break;
          }
        }
      }

      res = size;
    }

    hadoop__hdfs__get_block_locations_response_proto__free_unpacked(response, NULL);
    return res;
  }
}

static
struct fuse_operations hello_oper = {
  .getattr  = hadoop_fuse_getattr,
  .readdir  = hadoop_fuse_readdir,
  .readlink = hadoop_fuse_readlink,
  .chmod = hadoop_fuse_chmod,
  .rename = hadoop_fuse_rename,
  .unlink = hadoop_fuse_delete,
  .rmdir = hadoop_fuse_delete,
  .mkdir = hadoop_fuse_mkdir,
  .symlink = hadoop_fuse_symlink,
  .statfs = hadoop_fuse_statfs,
  .chown = hadoop_fuse_chown,
  .utimens = hadoop_fuse_utimens,
  .open   = hadoop_fuse_open,
  .read   = hadoop_fuse_read
};

int main(int argc, char *argv[])
{
  struct connection_state state;
  uint16_t port;
  int result;

  if (argc < 3)
  {
    fprintf(stderr, "need namenode host & namenode port as first two argumnets\n");
    return 1;
  }

#ifndef NDEBUG
  signal(SIGSEGV, dump_trace);
#endif

  port = atoi(argv[2]);
  memset(&state, 0, sizeof(state));
  result = hadoop_rpc_connect_namenode(&state, argv[1], port);
  if(result < 0)
  {
    fprintf(stderr, "connection to hdfs://%s:%d failed: %s\n", argv[1], port, strerror(-result));
    return 1;
  }

  argv[1] = "-s"; // always single-threaded
  for(int i = 3; i < argc; ++i)
  {
    argv[i - 1] = argv[i];
  }
  return fuse_main(argc - 1, argv, &hello_oper, &state);
}

