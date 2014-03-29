

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
#include <sys/time.h>
#include <unistd.h>
#ifndef NDEBUG
#include <execinfo.h>
#include <assert.h>
#include <syslog.h>
#endif

#include "proto/ClientNamenodeProtocol.pb-c.h"
#include "proto/datatransfer.pb-c.h"
#include "hadooprpc.h"
#include "minmax.h"

#ifndef NDEBUG
static void dump_trace() {
  void * buffer[255];
  const int calls = backtrace(buffer,
                              sizeof(buffer) / sizeof(void *));
  backtrace_symbols_fd(buffer, calls, 1);
  exit(EXIT_FAILURE);
}

#endif

// DECLARATIONS ----------------------------------------------

struct Hadoop_Fuse_FileHandle
{
  uint64_t fileid;
  uint64_t blocksize;
};

static
int hadoop_fuse_write(const char * src, const char * data, size_t len, off_t offset, struct fuse_file_info * fi);

static
int hadoop_fuse_ftruncate(const char * path, off_t offset, struct fuse_file_info * fi);

// HELPERS ---------------------------------------------------

static inline
struct namenode_state * hadoop_fuse_namenode_state()
{
  return (struct namenode_state *) fuse_get_context()->private_data;
}

/*
 * Not const as protobufs needs a char *.
 */
static inline
char * hadoop_fuse_client_name()
{
  return (char *) hadoop_fuse_namenode_state()->clientname;
}

#define CALL_NN(method, request, response) \
  hadoop_rpc_call_namenode( \
    hadoop_fuse_namenode_state(), \
    method, \
    (const ProtobufCMessage *) &request, \
    (ProtobufCMessage **) &response);

static
void unpack_filestatus(Hadoop__Hdfs__HdfsFileStatusProto * fs, struct stat * stbuf)
{
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
  else
  {
    stbuf->st_blksize = hadoop_fuse_namenode_state()->blocksize;
  }
  stbuf->st_mtime = fs->modification_time / 1000;
  stbuf->st_atime = fs->access_time / 1000;

  if(fs->owner)
  {
    struct passwd * o = getpwnam(fs->owner);
    if(o)
    {
      stbuf->st_uid = o->pw_uid;
    }
  }
  if(fs->group)
  {
    struct group * g = getgrnam(fs->group);
    if(g)
    {
      stbuf->st_gid = g->gr_gid;
    }
  }

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "unpack_filestatus, length=%llu blocksize=%zd",
    stbuf->st_size,
    stbuf->st_blksize);
#endif
}

static
void hadoop_fuse_clone_block(
  const Hadoop__Hdfs__ExtendedBlockProto * block,
  Hadoop__Hdfs__ExtendedBlockProto ** target)
{
  uint8_t * buf;
  size_t len = hadoop__hdfs__extended_block_proto__get_packed_size(block);

  if(*target)
  {
    hadoop__hdfs__extended_block_proto__free_unpacked(*target, NULL);
  }

  buf = alloca(len);
  hadoop__hdfs__extended_block_proto__pack(block, buf);
  *target = hadoop__hdfs__extended_block_proto__unpack(NULL, len, buf);
}

static
int hadoop_fuse_complete(const char * src, uint64_t fileid, Hadoop__Hdfs__ExtendedBlockProto * last)
{
  int res;
  Hadoop__Hdfs__CompleteRequestProto request = HADOOP__HDFS__COMPLETE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__CompleteResponseProto * response = NULL;

  request.src = (char *) src;
  request.clientname = hadoop_fuse_client_name();
  request.has_fileid = true;
  request.fileid = fileid;
  request.last = last;

  while(true)
  {
    bool complete;

    res = CALL_NN("complete", request, response);
    if(res < 0)
    {
      return res;
    }
    complete = response->result;
    hadoop__hdfs__complete_response_proto__free_unpacked(response, NULL);
    if(complete)
    {
      break;
    }
    else
    {
      sleep(1);
    }
  }

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_fuse_complete %s (fileid %llu) last block length = %llu",
    src,
    fileid,
    last ? last->numbytes : 0);
#endif

  return res;
}

static
int hadoop_fuse_createemptyfile(const char * src, mode_t perm, uint64_t * fileid)
{
  int res;
  Hadoop__Hdfs__CreateRequestProto request = HADOOP__HDFS__CREATE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__FsPermissionProto permission = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
  Hadoop__Hdfs__CreateResponseProto * response = NULL;

  request.src = (char *) src;
  request.clientname = hadoop_fuse_client_name();
  request.createparent = false;
  request.masked = &permission;
  permission.perm = perm;
  request.createflag = HADOOP__HDFS__CREATE_FLAG_PROTO__CREATE; // must not already exist
  request.replication = hadoop_fuse_namenode_state()->replication;
  request.blocksize = hadoop_fuse_namenode_state()->blocksize;

  res = CALL_NN("create", request, response);
  if(res < 0)
  {
    return res;
  }
  if(!response->fs || !response->fs->has_fileid)
  {
    res = -EPROTO;
    goto end;
  }

  res = hadoop_fuse_complete(src, response->fs->fileid, NULL);
  if(res < 0)
  {
    goto end;
  }

  res = 0;
  if(fileid)
  {
    *fileid = response->fs->fileid;
  }

end:
  hadoop__hdfs__create_response_proto__free_unpacked(response, NULL);
  return res;
}

static
int hadoop_fuse_lock(
  const char * src,
  Hadoop__Hdfs__ExtendedBlockProto ** last)
{
  int res;
  Hadoop__Hdfs__AppendRequestProto appendrequest = HADOOP__HDFS__APPEND_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__AppendResponseProto * appendresponse = NULL;

  // hadoop semantics means we must "append" to a file, even if we're
  // going to write in the middle of it. This makes us take the lease.

  appendrequest.src = (char *) src;
  appendrequest.clientname = hadoop_fuse_client_name();

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_fuse_lock appending to %s",
    src);
#endif

  res = CALL_NN("append", appendrequest, appendresponse);
  if(res < 0)
  {
    return res;
  }

  if(appendresponse->block)
  {
    hadoop_fuse_clone_block(appendresponse->block->b, last);
  }
  else
  {
    *last = NULL;
  }

  hadoop__hdfs__append_response_proto__free_unpacked(appendresponse, NULL);
  return 0;
}

static
int hadoop_fuse_write_block(
  const char * data, // if NULL, "len" \0s are written
  const off_t offset, // offset into block
  const size_t len, // len of data
  const Hadoop__Hdfs__ChecksumProto * checksum,
  const Hadoop__Hdfs__LocatedBlockProto * block,
  Hadoop__Hdfs__LocatedBlockProto * newblock)
{
  int res;
  Hadoop__Hdfs__ClientOperationHeaderProto clientheader = HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
  Hadoop__Hdfs__BaseHeaderProto baseheader = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
  Hadoop__Hdfs__OpWriteBlockProto oprequest = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__INIT;
  Hadoop__Hdfs__BlockOpResponseProto * opresponse = NULL;
  bool written = false;
  uint64_t newblocklen = max(newblock->b->has_numbytes ? newblock->b->numbytes : 0, offset + len);

  assert(len > 0);
  assert(offset >= 0);
  assert(block->n_locs > 0);

  clientheader.clientname = hadoop_fuse_client_name();
  baseheader.block = block->b;
  baseheader.token = newblock->blocktoken;
  clientheader.baseheader = &baseheader;
  oprequest.header = &clientheader;
  oprequest.pipelinesize = block->n_locs; // not actually used?
  if(newblock->b->generationstamp == block->b->generationstamp)
  {
    oprequest.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_CREATE;
  }
  else
  {
    oprequest.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_APPEND;
  }
  oprequest.latestgenerationstamp = newblock->b->generationstamp;
  oprequest.minbytesrcvd = newblock->b->numbytes;
  oprequest.maxbytesrcvd = newblocklen;
  oprequest.requestedchecksum = (Hadoop__Hdfs__ChecksumProto *) checksum;

  // targets are the other DNs in the pipeline that the DN we send our block
  // to will mirror the block to. Clearly this can't include the DN we're sending
  // to, as that would create a loop!
  oprequest.n_targets = block->n_locs - 1;
  oprequest.targets = alloca(oprequest.n_targets * sizeof(Hadoop__Hdfs__DatanodeInfoProto *));

  // for now, don't care about which location we send to, but we should probably
  // choose the "closest".
  for (size_t l = 0; l < block->n_locs && !written; ++l)
  {
    const Hadoop__Hdfs__DatanodeInfoProto * location = block->locs[l];
    struct connection_state dn_state;

    // build the target list without this location
    for(uint32_t t_idx = 0, l_idx = 0; l_idx < block->n_locs; ++l_idx)
    {
      if(l_idx != l)
      {
        oprequest.targets[t_idx++] = block->locs[l_idx];
      }
    }

    memset(&dn_state, 0, sizeof(dn_state));
    res = hadoop_rpc_connect_datanode(&dn_state, location->id->ipaddr, location->id->xferport);
    if(res < 0)
    {
      continue;
    }

    res = hadoop_rpc_call_datanode(&dn_state, 80, (const ProtobufCMessage *) &oprequest, &opresponse);
    if(res == 0)
    {
      hadoop__hdfs__block_op_response_proto__free_unpacked(opresponse, NULL);
      res = hadoop_rpc_send_packets(
        &dn_state,
        (uint8_t *) data,
        len,
        offset,
        hadoop_fuse_namenode_state()->packetsize,
        checksum);
      written = res == 0;
    }
    hadoop_rpc_disconnect(&dn_state);

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_write_block, %s %zd bytes (offset %zd) of block %s blk_%llu_%llu (was: %llu, now: %lli) to DN %s:%d (%zd of %zd)",
      (written ? "written" : "NOT written"),
      len,
      offset,
      block->b->poolid,
      block->b->blockid,
      newblock->b->generationstamp,
      newblock->b->numbytes,
      newblocklen,
      location->id->ipaddr,
      location->id->xferport,
      l + 1,
      block->n_locs);
#endif
  }

  if(written)
  {
    newblock->b->has_numbytes = true;
    newblock->b->numbytes = newblocklen;
    return len;
  }
  else
  {
    // ensure we return a sensible error code
    return res < 0 ? res : -EIO;
  }
}

/**
 * Assumes file is locked
 */
static
int hadoop_fuse_do_write(
  const char * src,
  const char * const data,
  const size_t len,
  const uint64_t offsetintofile,
  struct Hadoop_Fuse_FileHandle * fh,
  Hadoop__Hdfs__LocatedBlocksProto * locations,
  Hadoop__Hdfs__ExtendedBlockProto ** last)
{
  int res;
  Hadoop__Hdfs__ChecksumProto checksum = HADOOP__HDFS__CHECKSUM_PROTO__INIT;
  uint32_t numblocks;
  uint64_t byteswritten = 0;
  assert(fh);
  assert(fh->blocksize);

  numblocks = locations ? locations->n_blocks : 0;

  // (2) set up the checksum algorithm we'll use to transfer the data
  checksum.type = HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL;
  checksum.bytesperchecksum = hadoop_fuse_namenode_state()->bytesperchecksum;

  // (3) loop through the blocks, seeing if we have to overwrite any.
  for(size_t b = 0; b < numblocks; ++b)
  {
    Hadoop__Hdfs__LocatedBlockProto * block = locations->blocks[b];
    uint64_t blockstart = block->offset;
    uint64_t blockend;
    uint64_t writetothisblock;
    uint64_t offsetintoblock = max(offsetintofile - blockstart, 0);

    assert(block->b->has_numbytes); // not sure where to get this from otherwise

    if(b == numblocks - 1)
    {
      // the last block
      blockend = blockstart + fh->blocksize;
    }
    else
    {
      blockend = blockstart + block->b->numbytes;
    }

    if(offsetintofile > blockend || offsetintofile + len < blockstart)
    {
      // definitely not here:
      continue;
    }

    writetothisblock = min(blockend - max(offsetintofile + byteswritten, blockstart), len - byteswritten);

    if(writetothisblock > 0)
    {
      Hadoop__Hdfs__UpdateBlockForPipelineRequestProto updateblockrequest = HADOOP__HDFS__UPDATE_BLOCK_FOR_PIPELINE_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__UpdateBlockForPipelineResponseProto * updateblockresponse = NULL;
      Hadoop__Hdfs__UpdatePipelineRequestProto updaterequest = HADOOP__HDFS__UPDATE_PIPELINE_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__UpdatePipelineResponseProto * updateresponse = NULL;
      Hadoop__Hdfs__DatanodeIDProto ** newnodes = NULL;

      updateblockrequest.clientname = hadoop_fuse_client_name();
      updateblockrequest.block = block->b;

      // since we're updating a block, we need to get a new "generation stamp" (version)
      // from the NN.
      res = CALL_NN("updateBlockForPipeline", updateblockrequest, updateblockresponse);
      if(res < 0)
      {
        goto endwrite;
      }

#ifndef NDEBUG
      syslog(
        LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
        "hadoop_fuse_do_write got version %llu for block %s blk_%llu_%llu on %zd node(s) (writing %llu bytes to block offset %llu, file offset %llu)",
        updateblockresponse->block->b->generationstamp,
        block->b->poolid,
        block->b->blockid,
        block->b->generationstamp,
        block->n_locs,
        writetothisblock,
        offsetintoblock,
        offsetintofile);
#endif

      res = hadoop_fuse_write_block(
        data ? (data + byteswritten) : NULL,
        offsetintoblock,
        writetothisblock,
        &checksum,
        block,
        updateblockresponse->block);
      if(res < 0)
      {
        goto endwrite;
      }

      // tell the NN we've updated the block

      newnodes = alloca(block->n_locs * sizeof(*newnodes));
      for(size_t n = 0; n < block->n_locs; ++n)
      {
        newnodes[n] = block->locs[n]->id;
      }

      updaterequest.clientname = hadoop_fuse_client_name();
      updaterequest.oldblock = block->b;
      updaterequest.newblock = updateblockresponse->block->b;
      updaterequest.n_newnodes = block->n_locs;
      updaterequest.newnodes = newnodes;
      updaterequest.n_storageids = block->n_storageids;
      updaterequest.storageids = block->storageids;

      res = CALL_NN("updatePipeline", updaterequest, updateresponse);
      if(res < 0)
      {
        goto endwrite;
      }

      hadoop_fuse_clone_block(updateblockresponse->block->b, last);

endwrite:
      if(updateblockresponse)
      {
        hadoop__hdfs__update_block_for_pipeline_response_proto__free_unpacked(updateblockresponse, NULL);
      }
      if(updateresponse)
      {
        hadoop__hdfs__update_pipeline_response_proto__free_unpacked(updateresponse, NULL);
      }
      if(res < 0)
      {
        return res;
      }
      else
      {
        byteswritten += writetothisblock;
      }
    }
  }

  // (4) if we've still got some more to write, keep tacking on blocks
  //     and filling them
  while(byteswritten < len)
  {
    Hadoop__Hdfs__AddBlockRequestProto block_request = HADOOP__HDFS__ADD_BLOCK_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__AddBlockResponseProto * block_response = NULL;
    uint64_t writetothisblock = min(len - byteswritten, fh->blocksize);

    block_request.src = (char *) src;
    block_request.clientname = hadoop_fuse_client_name();
    block_request.previous = *last;
    block_request.has_fileid = true;
    block_request.fileid = fh->fileid;
    block_request.n_excludenodes = 0;
    block_request.n_favorednodes = 0;

    res = CALL_NN("addBlock", block_request, block_response);
    if(res < 0)
    {
      return res;
    }

    res = hadoop_fuse_write_block(
      data ? (data + byteswritten) : NULL,
      0, // block offset
      writetothisblock,
      &checksum,
      block_response->block,
      block_response->block);
    if(res < 0)
    {
      hadoop__hdfs__add_block_response_proto__free_unpacked(block_response, NULL);
      return res;
    }

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_do_write added new block %s blk_%llu_%llu on %zd node(s) (writing %llu bytes to block offset 0)",
      block_response->block->b->poolid,
      block_response->block->b->blockid,
      block_response->block->b->generationstamp,
      block_response->block->n_locs,
      writetothisblock);
#endif

    byteswritten += writetothisblock;
    hadoop_fuse_clone_block(block_response->block->b, last);

    hadoop__hdfs__add_block_response_proto__free_unpacked(block_response, NULL);
  }

  return 0;
}

// FUSE OPERATIONS -------------------------------------------

static
int hadoop_fuse_getattr(const char * path, struct stat * stbuf)
{
  int res;
  Hadoop__Hdfs__GetFileInfoRequestProto request = HADOOP__HDFS__GET_FILE_INFO_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFileInfoResponseProto * response = NULL;

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
  Hadoop__Hdfs__GetLinkTargetResponseProto * response = NULL;

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
  Hadoop__Hdfs__SetTimesResponseProto * response = NULL;

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
  Hadoop__Hdfs__SetOwnerResponseProto * response = NULL;

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
  Hadoop__Hdfs__CreateSymlinkResponseProto * response = NULL;

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
  Hadoop__Hdfs__RenameResponseProto * response = NULL;

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
  Hadoop__Hdfs__SetPermissionResponseProto * response = NULL;

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
  uint64_t blocksize;
  (void) src; // HDFS only supports getPreferredBlockSize for files
  Hadoop__Hdfs__GetFsStatusRequestProto fs_request = HADOOP__HDFS__GET_FS_STATUS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFsStatsResponseProto * fs_response = NULL;

  res = CALL_NN("getFsStats", fs_request, fs_response);
  if(res < 0)
  {
    return res;
  }

  blocksize = hadoop_fuse_namenode_state()->blocksize;
  stat->f_bsize = stat->f_frsize = blocksize;
  stat->f_blocks = fs_response->capacity / blocksize;
  stat->f_bfree = stat->f_bavail = fs_response->remaining / blocksize;
  stat->f_namemax = 0x7FFFFFFF; // java String max length

  hadoop__hdfs__get_fs_stats_response_proto__free_unpacked(fs_response, NULL);
  return 0;
}

static
int hadoop_fuse_delete(const char * src)
{
  int res;
  Hadoop__Hdfs__DeleteRequestProto request = HADOOP__HDFS__DELETE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__DeleteResponseProto * response = NULL;

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
  Hadoop__Hdfs__MkdirsResponseProto * response = NULL;

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
int hadoop_fuse_readdir(const char * path, void * buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info * fi)
{
  (void) offset;
  (void) fi;
  int res;
  Hadoop__Hdfs__GetListingRequestProto request = HADOOP__HDFS__GET_LISTING_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetListingResponseProto * response = NULL;

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

    filename = alloca(file->path.len + 1);
    filename[file->path.len] = '\0';
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
int hadoop_fuse_fsync(const char * src, int datasync, struct fuse_file_info * fi)
{
  (void) datasync;
  (void) fi;
  int res;
  Hadoop__Hdfs__FsyncRequestProto request = HADOOP__HDFS__FSYNC_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__FsyncResponseProto * response = NULL;

  request.src = (char *) src;
  request.client = hadoop_fuse_client_name();
  res = CALL_NN("fsync", request, response);
  if(res < 0)
  {
    return res;
  }
  else
  {
    hadoop__hdfs__fsync_response_proto__free_unpacked(response, NULL);
    return 0;
  }
}

static
int hadoop_fuse_mknod(const char * src, mode_t perm, dev_t dev)
{
  (void) dev;

  return hadoop_fuse_createemptyfile(src, perm, NULL);
}

static
int hadoop_fuse_open(const char * path, struct fuse_file_info * fi)
{
  int res;
  Hadoop__Hdfs__GetFileInfoRequestProto file_request = HADOOP__HDFS__GET_FILE_INFO_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetFileInfoResponseProto * file_response = NULL;
  Hadoop__Hdfs__FsPermissionProto permission = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
  uint64_t fileid; // of the existing file, or 0
  int flags = fi->flags;
  uint64_t blocksize = hadoop_fuse_namenode_state()->blocksize;
  struct Hadoop_Fuse_FileHandle * fh;

  file_request.src = (char *) path;
  res = CALL_NN("getFileInfo", file_request, file_response);
  if(res < 0)
  {
    return res;
  }

  if(file_response->fs && file_response->fs->has_fileid)
  {
    fileid = file_response->fs->fileid;
    permission.perm = file_response->fs->permission->perm;
    if(file_response->fs->has_blocksize && file_response->fs->blocksize)
    {
      blocksize = file_response->fs->blocksize;
    }

    hadoop__hdfs__get_file_info_response_proto__free_unpacked(file_response, NULL);

    if((flags & O_EXCL) == O_EXCL)
    {
      // exclusive - but the file already exists? fail:
      return -EEXIST;
    }
  }
  else
  {
    hadoop__hdfs__get_file_info_response_proto__free_unpacked(file_response, NULL);

    if(flags & O_CREAT)
    {
      res = hadoop_fuse_createemptyfile(path, 0755, &fileid);
      if(res < 0)
      {
        return res;
      }
    }
    else
    {
      // not there, and we don't want to create it
      return -ENOENT;
    }
  }

  // set up our file handle info
  fh = malloc(sizeof(*fh));
  if(!fh)
  {
    return -ENOMEM;
  }
  memset(fh, 0, sizeof(*fh));
  fi->fh = (uint64_t) fh;
  fh->fileid = fileid;
  fh->blocksize = blocksize;

  if(flags & O_TRUNC)
  {
    res = hadoop_fuse_ftruncate(path, 0, fi);
    if(res < 0)
    {
      return res;
    }
  }

  return 0;
}

static
int hadoop_fuse_ftruncate(const char * path, off_t offset, struct fuse_file_info * fi)
{
  int res;
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto * response = NULL;
  Hadoop__Hdfs__ExtendedBlockProto * last = NULL;
  struct Hadoop_Fuse_FileHandle * fh = (struct Hadoop_Fuse_FileHandle *) fi->fh;
  uint64_t oldlength;
  uint64_t newlength = offset;

  assert(offset >= 0);

  request.src = (char *) path;
  request.offset = 0;
  request.length = 0x7FFFFFFFFFFFFFFF; // not really a uint64_t due to Java
  res = CALL_NN("getBlockLocations", request, response);
  if(res < 0)
  {
    return res;
  }
  if(!response->locations)
  {
    res = -ENOENT;
    goto end;
  }
  oldlength = response->locations->filelength;

  res = hadoop_fuse_lock(path, &last);
  if(res < 0)
  {
    goto end;
  }

  if(newlength > oldlength)
  {
    // extend the current file by appending NULLs

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_ftruncate, writing %llu bytes to %s to ftruncate size up to %zd",
      newlength - oldlength,
      path,
      offset);
#endif

    res = hadoop_fuse_do_write(
      path,
      NULL, // append \0s
      newlength - oldlength,
      oldlength,
      (struct Hadoop_Fuse_FileHandle *) fi->fh,
      response->locations,
      &last);
    if(res < 0)
    {
      goto end;
    }
  }
  else
  {
    Hadoop__Hdfs__AbandonBlockRequestProto abandon_request = HADOOP__HDFS__ABANDON_BLOCK_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__AbandonBlockResponseProto * abandon_response = NULL;

    abandon_request.src = (char *) path;
    abandon_request.holder = hadoop_fuse_client_name();

    for(uint32_t b = response->locations->n_blocks; b > 0; --b)
    {
      Hadoop__Hdfs__LocatedBlockProto * block = response->locations->blocks[b - 1];

      if(block->offset + block->b->numbytes <= newlength)
      {
        // we need the whole of this block, and since we're iterating
        // through the file backwards we need all subsequent blocks.
        break;
      }

      if(block->offset < newlength)
      {
        // we need a bit of this block
        assert(false);
      }
      else
      {
        // the block starts on or after the new length, so toss the whole
        // thing

        abandon_request.b = block->b;

#ifndef NDEBUG
        syslog(
          LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
          "hadoop_fuse_ftruncate, dropping block %s blk_%llu_%llu of %s as start %llu >= offset %llu",
          block->b->poolid,
          block->b->blockid,
          block->b->generationstamp,
          path,
          block->offset,
          newlength);
#endif

        res = CALL_NN("abandonBlock", abandon_request, abandon_response);
        if(res < 0)
        {
          goto end;
        }

        hadoop_fuse_clone_block(block->b, &last);
        last->has_numbytes = true;
        last->numbytes = 0;
        hadoop__hdfs__abandon_block_response_proto__free_unpacked(abandon_response, NULL);
      }
    }
  }

  // persist the data we've just written, & release the lease we have on "src"
  res = hadoop_fuse_complete(path, fh->fileid, last);
  if(res < 0)
  {
    goto end;
  }

  res = 0;
end:
  hadoop__hdfs__get_block_locations_response_proto__free_unpacked(response, NULL);
  if(last)
  {
    hadoop__hdfs__extended_block_proto__free_unpacked(last, NULL);
  }
  return res;
}

static
int hadoop_fuse_truncate(const char * path, off_t offset)
{
  return hadoop_fuse_ftruncate(path, offset, NULL);
}

static
int hadoop_fuse_write(
  const char * src,
  const char * const data,
  const size_t len,
  const off_t offset,
  struct fuse_file_info * fi)
{
  int res;
  int error = len; // ideally we'll write all the input
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto * response = NULL;
  Hadoop__Hdfs__ExtendedBlockProto * last = NULL;
  struct Hadoop_Fuse_FileHandle * fh = (struct Hadoop_Fuse_FileHandle *) fi->fh;

  request.src = (char *) src;
  request.offset = 0;
  request.length = 0x7FFFFFFFFFFFFFFF; // not really a uint64_t due to Java
  res = CALL_NN("getBlockLocations", request, response);
  if(res < 0)
  {
    error = res;
    goto end;
  }
  if(!response->locations)
  {
    error = -ENOENT;
    goto end;
  }

  res = hadoop_fuse_lock(src, &last);
  if(res < 0)
  {
    error = res;
    goto end;
  }

  res = hadoop_fuse_do_write(
    src,
    data,
    len,
    (uint64_t) offset,
    fh,
    response->locations,
    &last);
  if(res < 0)
  {
    error = res;
    // the write failed, but we still need to unlock the file by
    // "completing" it.
    // goto end;
  }

  // persist the data we've just written, & release the lease we have on "src"
  res = hadoop_fuse_complete(src, fh->fileid, last);
  if(res < 0)
  {
    error = res;
    goto end;
  }

end:
  if(response)
  {
    hadoop__hdfs__get_block_locations_response_proto__free_unpacked(response, NULL);
  }
  if(last)
  {
    // these are clones, so we have to free them
    hadoop__hdfs__extended_block_proto__free_unpacked(last, NULL);
  }
  return error;
}

static
int hadoop_fuse_read(const char * src, char * buf, size_t size, off_t offset, struct fuse_file_info * fi)
{
  (void) buf;
  (void) fi;
  int res;
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto * response = NULL;

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
      // This means that some (other?) client has the lease on this
      // file. Try again later - maybe they'll have release it?
      res = -EAGAIN;
    }
    else
    {
      Hadoop__Hdfs__ClientOperationHeaderProto clientheader = HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
      Hadoop__Hdfs__BaseHeaderProto baseheader = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
      Hadoop__Hdfs__OpReadBlockProto op = HADOOP__HDFS__OP_READ_BLOCK_PROTO__INIT;

      clientheader.clientname = hadoop_fuse_client_name();

      for (size_t b = 0; b < locations->n_blocks; ++b)
      {
        Hadoop__Hdfs__LocatedBlockProto * block = locations->blocks[b];

        if(block->corrupt)
        {
          res = -EBADMSG;
        }
        else
        {
          Hadoop__Hdfs__BlockOpResponseProto * opresponse = NULL;
          bool written = false;

          baseheader.block = block->b;
          clientheader.baseheader = &baseheader;
          op.header = &clientheader;
          op.has_sendchecksums = true;
          op.sendchecksums = false;
          op.offset = min(offset - block->offset, 0); // offset into file -> offset into block
          op.len = min(block->b->numbytes - op.offset, size);

          // for now, don't care about which location we get it from
          for (size_t l = 0; l < block->n_locs; ++l)
          {
            Hadoop__Hdfs__DatanodeInfoProto * location = block->locs[l];
            struct connection_state dn_state;
            uint64_t skipbytes;

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
            if(opresponse->readopchecksuminfo)
            {
              skipbytes =  op.offset - opresponse->readopchecksuminfo->chunkoffset;
            }
            else
            {
              skipbytes = 0;
            }
            hadoop__hdfs__block_op_response_proto__free_unpacked(opresponse, NULL);

            res = hadoop_rpc_receive_packets(
              &dn_state,
              skipbytes,
              op.len,
              (uint8_t *) buf);
            if(res < 0)
            {
              hadoop_rpc_disconnect(&dn_state);
              continue;
            }

            written = true;
            hadoop_rpc_disconnect(&dn_state);
            break;
          }

          if(!written)
          {
            return -EIO;
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
void * hadoop_fuse_init(struct fuse_conn_info * conn)
{
  conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
  conn->capable |= FUSE_CAP_ATOMIC_O_TRUNC;
  return fuse_get_context()->private_data;
}

static
struct fuse_operations hello_oper = {
  .getattr = hadoop_fuse_getattr,
  .readdir = hadoop_fuse_readdir,
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
  .open = hadoop_fuse_open,
  .read = hadoop_fuse_read,
  .fsync = hadoop_fuse_fsync,
  .mknod = hadoop_fuse_mknod,
  .write = hadoop_fuse_write,
  .truncate = hadoop_fuse_truncate,
  .ftruncate = hadoop_fuse_ftruncate,
  .init = hadoop_fuse_init
};

int main(int argc, char * argv[])
{
  struct namenode_state state;
  uint16_t port;
  int res;

  if (argc < 3)
  {
    fprintf(stderr, "need namenode host & namenode port as first two arguments\n");
    return 1;
  }

  port = atoi(argv[2]);
  memset(&state, 0, sizeof(state));
  state.clientname = "hadoop_fuse";
  pthread_mutex_init(&state.connection.mutex, NULL);

#ifndef NDEBUG
  openlog (state.clientname, LOG_CONS | LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
  signal(SIGSEGV, dump_trace);
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "starting, connecting to hdfs://%s:%d", argv[1], port);
#endif

  res = hadoop_rpc_connect_namenode(&state, argv[1], port);
  if(res < 0)
  {
    fprintf(stderr, "connection to hdfs://%s:%d failed: %s\n", argv[1], port, strerror(-res));
    return 1;
  }

  for(int i = 3; i < argc; ++i)
  {
    argv[i - 2] = argv[i];
  }
  res = fuse_main(argc - 2, argv, &hello_oper, &state);

#ifndef NDEBUG
  closelog();
#endif

  return res;
}

