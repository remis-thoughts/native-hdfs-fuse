

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
#include <signal.h>
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

static
int hadoop_fuse_read(const char * src, char * buf, size_t size, off_t offset, struct fuse_file_info * fi);

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
  default:
    break;
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
    "unpack_filestatus, length=%" PRIu64 " blocksize=%zd",
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
  size_t len;

  if(*target)
  {
    hadoop__hdfs__extended_block_proto__free_unpacked(*target, NULL);
  }

  if(block)
  {
    len = hadoop__hdfs__extended_block_proto__get_packed_size(block);
    buf = alloca(len);
    hadoop__hdfs__extended_block_proto__pack(block, buf);
    *target = hadoop__hdfs__extended_block_proto__unpack(NULL, len, buf);
  }
  else
  {
    *target = NULL;
  }
}

static
int hadoop_fuse_complete(const char * src, uint64_t fileid, Hadoop__Hdfs__ExtendedBlockProto * last)
{
  int res;
  Hadoop__Hdfs__CompleteRequestProto request = HADOOP__HDFS__COMPLETE_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__CompleteResponseProto * response = NULL;

  request.src = (char *) src;
  request.clientname = hadoop_fuse_client_name();
  request.has_fileid = fileid > 0;
  request.fileid = fileid;
  request.last = last;

  assert(src);

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
  if(last)
  {
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_complete %s (fileid %" PRIu64 ") last block %s blk_%" PRIu64 "_%" PRIu64 " (length = %" PRIu64 ") => %d",
      src,
      fileid,
      last->poolid,
      last->blockid,
      last->generationstamp,
      last->numbytes,
      res);
  }
  else
  {
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_complete %s (fileid %" PRIu64 ") with NO last block => %d",
      src,
      fileid,
      res);
  }
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

  res = CALL_NN("append", appendrequest, appendresponse);
  if(res < 0)
  {
    goto end;
  }

  if(appendresponse->block)
  {
    hadoop_fuse_clone_block(appendresponse->block->b, last);
  }

  res = 0;
  hadoop__hdfs__append_response_proto__free_unpacked(appendresponse, NULL);

end:

#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_fuse_lock %s => %d",
    src,
    res);
#endif

  return res;
}

static
int hadoop_fuse_write_block(
  struct Hadoop_Fuse_Buffer_Pos * bufferpos,
  const uint64_t len, // bytes to write
  const off_t blockoffset, // offset into block
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
  uint64_t newblocklen = max(newblock->b->has_numbytes ? newblock->b->numbytes : 0, blockoffset + len);

  assert(bufferpos->len > 0);
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
        bufferpos,
        len,
        blockoffset,
        hadoop_fuse_namenode_state()->packetsize,
        checksum);
      written = res == 0;
    }
    hadoop_rpc_disconnect(&dn_state);

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_write_block, %s %" PRIu64 " bytes to block offset %" PRIu64 " of block %s blk_%" PRIu64 "_%" PRIu64 " (was: %" PRIu64 ", now: %" PRIu64 ") to DN %s:%d (%zd of %zd) => %d",
      (written ? "written" : "NOT written"),
      len,
      blockoffset,
      block->b->poolid,
      block->b->blockid,
      newblock->b->generationstamp,
      newblock->b->numbytes,
      newblocklen,
      location->id->ipaddr,
      location->id->xferport,
      l + 1,
      block->n_locs,
      res);
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

static inline
uint64_t hadoop_fuse_bytestowrite(
  const struct Hadoop_Fuse_Buffer_Pos * bufferpos,
  const uint64_t blockoffset,
  const struct Hadoop_Fuse_FileHandle * fh)
{
  return min(fh->blocksize - blockoffset, bufferpos->len - bufferpos->bufferoffset);
}

/**
 * Assumes file is locked, and offsetintofile is in the (under construction)
 * last block of the file. offsetintofile must also be contiguous, i.e. at most
 * current file length + 1
 */
static
int hadoop_fuse_do_write(
  const char * src,
  struct Hadoop_Fuse_Buffer_Pos * bufferpos,
  const uint64_t offsetintofile,
  const struct Hadoop_Fuse_FileHandle * fh,
  const Hadoop__Hdfs__LocatedBlockProto * lastlocation,
  Hadoop__Hdfs__ExtendedBlockProto ** last)
{
  int res = 0;
  Hadoop__Hdfs__ChecksumProto checksum = HADOOP__HDFS__CHECKSUM_PROTO__INIT;

  assert(fh);
  assert(fh->blocksize);
  assert(!lastlocation || lastlocation->offset <= offsetintofile);
  assert(!lastlocation || lastlocation->b->has_numbytes); // not sure where to get this from otherwise
  assert(bufferpos);
  assert(bufferpos->bufferoffset == 0);

  // set up the checksum algorithm we'll use to transfer the data
  checksum.type = hadoop_fuse_namenode_state()->checksumtype;
  checksum.bytesperchecksum = hadoop_fuse_namenode_state()->bytesperchecksum;

  if(lastlocation && offsetintofile < lastlocation->offset + fh->blocksize && offsetintofile >= lastlocation->offset)
  {
    Hadoop__Hdfs__UpdateBlockForPipelineRequestProto updateblockrequest = HADOOP__HDFS__UPDATE_BLOCK_FOR_PIPELINE_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__UpdateBlockForPipelineResponseProto * updateblockresponse = NULL;
    Hadoop__Hdfs__UpdatePipelineRequestProto updaterequest = HADOOP__HDFS__UPDATE_PIPELINE_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__UpdatePipelineResponseProto * updateresponse = NULL;
    Hadoop__Hdfs__DatanodeIDProto ** newnodes = NULL;
    uint64_t blockoffset = offsetintofile - lastlocation->offset;
    uint64_t len = hadoop_fuse_bytestowrite(bufferpos, blockoffset, fh);

    updateblockrequest.clientname = hadoop_fuse_client_name();
    updateblockrequest.block = lastlocation->b;

    // since we're updating a block, we need to get a new "generation stamp" (version)
    // from the NN.
    res = CALL_NN("updateBlockForPipeline", updateblockrequest, updateblockresponse);
    if(res < 0)
    {
      goto endwrite;
    }

    res = hadoop_fuse_write_block(
      bufferpos,
      len,
      blockoffset,
      &checksum,
      lastlocation,
      updateblockresponse->block);
    if(res < 0)
    {
      goto endwrite;
    }

    // tell the NN we've updated the block

    newnodes = alloca(lastlocation->n_locs * sizeof(Hadoop__Hdfs__DatanodeIDProto *));
    for(size_t n = 0; n < lastlocation->n_locs; ++n)
    {
      newnodes[n] = lastlocation->locs[n]->id;
    }

    updaterequest.clientname = hadoop_fuse_client_name();
    updaterequest.oldblock = lastlocation->b;
    updaterequest.newblock = updateblockresponse->block->b;
    updaterequest.n_newnodes = lastlocation->n_locs;
    updaterequest.newnodes = newnodes;
    updaterequest.n_storageids = lastlocation->n_storageids;
    updaterequest.storageids = lastlocation->storageids;

    res = CALL_NN("updatePipeline", updaterequest, updateresponse);
    if(res < 0)
    {
      goto endwrite;
    }

    hadoop_fuse_clone_block(updateblockresponse->block->b, last);

endwrite:
#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_do_write wrote new version %" PRIu64 " for block %s blk_%" PRIu64 "_%" PRIu64 " on %zd node(s) writing %" PRIu64 " bytes to block offset %" PRIu64 ", file offset %" PRIu64 ", block is %" PRIu64 "-%" PRIu64 " => %d",
      updateblockresponse->block->b->generationstamp,
      lastlocation->b->poolid,
      lastlocation->b->blockid,
      lastlocation->b->generationstamp,
      lastlocation->n_locs,
      len,
      offsetintofile - lastlocation->offset,
      offsetintofile,
      lastlocation->offset,
      lastlocation->offset + fh->blocksize,
      res);
#endif
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
      return res;   // stop writing blocks
    }
  }

  // TODO: fill nulls if offset after last byte.

  // (4) if we've still got some more to write, keep tacking on blocks
  //     and filling them
  while(bufferpos->bufferoffset < bufferpos->len && res >= 0)
  {
    Hadoop__Hdfs__AddBlockRequestProto block_request = HADOOP__HDFS__ADD_BLOCK_REQUEST_PROTO__INIT;
    Hadoop__Hdfs__AddBlockResponseProto * block_response = NULL;
    uint64_t len =  hadoop_fuse_bytestowrite(bufferpos, 0, fh);

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
      bufferpos,
      len,
      0, // block offset
      &checksum,
      block_response->block,
      block_response->block);
    if(res < 0)
    {
      // we failed to write data into our new block. We'll try and keep HDFS
      // consisitent, so we'll tell the NN to throw away this failed block.
      // This may well work, as "hadoop_fuse_write_block" only talks to a DN, so
      // even if that fails there's every chance the NN will still be responsive.
      Hadoop__Hdfs__AbandonBlockRequestProto abandon_request = HADOOP__HDFS__ABANDON_BLOCK_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__AbandonBlockResponseProto * abandon_response = NULL;

      abandon_request.src = (char *) src;
      abandon_request.holder = hadoop_fuse_client_name();
      abandon_request.b = block_response->block->b;

      CALL_NN("abandonBlock", abandon_request, abandon_response); // ignore return value.
      if(abandon_response)
      {
        hadoop__hdfs__abandon_block_response_proto__free_unpacked(abandon_response, NULL);
      }
    }
    else
    {
#ifndef NDEBUG
      syslog(
        LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
        "hadoop_fuse_do_write added new block %s blk_%" PRIu64 "_%" PRIu64 " on %zd node(s) after %s blk_%" PRIu64 "_%" PRIu64 " & now written %zu bytes => %d",
        block_response->block->b->poolid,
        block_response->block->b->blockid,
        block_response->block->b->generationstamp,
        block_response->block->n_locs,
        *last ? (*last)->poolid : "",
        *last ? (*last)->blockid : 0,
        *last ? (*last)->generationstamp : 0,
        bufferpos->bufferoffset,
        res);
#endif

      hadoop_fuse_clone_block(block_response->block->b, last);
    }

    hadoop__hdfs__add_block_response_proto__free_unpacked(block_response, NULL);
  }

  return res;
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
  char * filename = NULL;
  size_t filenamelen = 0;

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

    if(filenamelen < file->path.len + 1)
    {
      //need more space
      filenamelen = file->path.len + 1;

      if(filename)
      {
        filename = realloc(filename, filenamelen);
      }
      else
      {
        filename = malloc(filenamelen);
      }
    }
    filename[file->path.len] = '\0';
    strncpy(filename, (const char *) file->path.data, file->path.len);
    int filled = filler(buf, filename, forfuse, 0);
    if(filled != 0)
    {
      return -ENOMEM;
    }
  }

  free(filename);
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
  fh = calloc(1, sizeof(*fh));
  if(!fh)
  {
    return -ENOMEM;
  }
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
  uint32_t n_blocks;

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
  n_blocks = response->locations ? response->locations->n_blocks : 0;

  if(newlength > oldlength)
  {
    // extend the current file by appending NULLs

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_ftruncate, writing %" PRIu64 " bytes to %s to ftruncate size up to %" PRIu64 " (from %" PRIu64 ")",
      newlength - oldlength,
      path,
      newlength,
      oldlength);
#endif

    res = hadoop_fuse_write(
      path,
      NULL, // append \0s
      newlength - oldlength,
      oldlength,
      fi);
    if(res < 0)
    {
      goto end;
    }
  }
  else if(newlength == oldlength)
  {
#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_ftruncate making no changes to %s (size %" PRIu64 ")",
      path,
      oldlength);
#endif
    goto end;
  }
  else
  {
    struct Hadoop_Fuse_Buffer byteskept = {
      .data = NULL,
      .len = 0
    };
    uint64_t bytesoffset = 0;
    uint32_t lastkeptblock = n_blocks;

    for(uint32_t b = 0; b < n_blocks; ++b)
    {
      Hadoop__Hdfs__LocatedBlockProto * block = response->locations->blocks[b];
      uint64_t blockstart = block->offset;
      uint64_t blockend = blockstart + block->b->numbytes;

      if(blockend <= newlength)
      {
        // we need the whole of this block...
        lastkeptblock = b;
        continue;
      }

      if(blockstart < newlength)
      {
        // we need some bits of this block
        byteskept.len = newlength - blockstart;

        // we need a bit of this block (this block of code should
        // only be executed for one of the blocks in the file). However,
        // HDFS blocks are append-only so we have to get the data we want
        // to preserve, abandon the old (longer) block, add a new (shorter)
        // block and put the data back.
        byteskept.data = malloc(byteskept.len);
        if(!byteskept.data)
        {
          res = -ENOMEM;
          goto end;
        }

        res = hadoop_fuse_read(path, byteskept.data, byteskept.len, block->offset, fi);
        if(res < 0)
        {
          goto end;
        }
        bytesoffset = blockstart; // where to put them back
      }

      break;
    }

    if(lastkeptblock == n_blocks)
    {
      // we're dropping all of them
      Hadoop__Hdfs__GetFileInfoRequestProto filerequest = HADOOP__HDFS__GET_FILE_INFO_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__GetFileInfoResponseProto * fileresponse = NULL;
      Hadoop__Hdfs__CreateRequestProto overwriterequest = HADOOP__HDFS__CREATE_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__CreateResponseProto * overwriteresponse = NULL;

      filerequest.src = (char *) path;
      res = CALL_NN("getFileInfo", filerequest, fileresponse);
      if(res < 0)
      {
        return res;
      }

      overwriterequest.src = (char *) path;
      overwriterequest.clientname = hadoop_fuse_client_name();
      overwriterequest.createparent = false;
      overwriterequest.masked = fileresponse->fs->permission;
      overwriterequest.createflag = HADOOP__HDFS__CREATE_FLAG_PROTO__OVERWRITE; // must already exist
      overwriterequest.replication = fileresponse->fs->block_replication;
      overwriterequest.blocksize = fileresponse->fs->blocksize;

#ifndef NDEBUG
      syslog(
        LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
        "hadoop_fuse_ftruncate, overwriting %s",
        path);
#endif

      res = CALL_NN("create", overwriterequest, overwriteresponse);
      hadoop__hdfs__get_file_info_response_proto__free_unpacked(fileresponse, NULL);
      if(res < 0)
      {
        return res;
      }
      if(overwriteresponse->fs->has_fileid)
      {
        fh->fileid = overwriteresponse->fs->fileid;
      }
      hadoop__hdfs__create_response_proto__free_unpacked(overwriteresponse, NULL);

      hadoop_fuse_clone_block(NULL, &last);
    }
    else
    {
      // put the abandon block messages here as we may make more than one call
      Hadoop__Hdfs__AbandonBlockRequestProto abandon_request = HADOOP__HDFS__ABANDON_BLOCK_REQUEST_PROTO__INIT;
      Hadoop__Hdfs__AbandonBlockResponseProto * abandon_response = NULL;

      abandon_request.src = (char *) path;
      abandon_request.holder = hadoop_fuse_client_name();

      // we need to lock the file to drop blocks, and we'll always drop
      // at least one block as the new size is strictly less than the old
      // size.
      res = hadoop_fuse_lock(path, &last);
      if(res < 0)
      {
        goto end;
      }

      for(uint32_t b = n_blocks - 1; b > lastkeptblock; --b)
      {
        Hadoop__Hdfs__LocatedBlockProto * block = response->locations->blocks[b];

        abandon_request.b = block->b;

#ifndef NDEBUG
        syslog(
          LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
          "hadoop_fuse_ftruncate, dropping block %s blk_%" PRIu64 "_%" PRIu64 " of %s as start %" PRIu64 " + len %" PRIu64 " >= new len %" PRIu64 " (keeping %zd bytes)",
          block->b->poolid,
          block->b->blockid,
          block->b->generationstamp,
          path,
          block->offset,
          block->b->numbytes,
          newlength,
          byteskept.len);
#endif

        res = CALL_NN("abandonBlock", abandon_request, abandon_response);
        if(res < 0)
        {
          goto end;
        }
        hadoop__hdfs__abandon_block_response_proto__free_unpacked(abandon_response, NULL);
      }

      hadoop_fuse_clone_block(response->locations->blocks[lastkeptblock]->b, &last);
    }

    if(byteskept.data)
    {
      struct Hadoop_Fuse_Buffer_Pos bufferpos = {
        .buffers = &byteskept,
        .n_buffers = 1,
        .bufferoffset = 0,
        .len = byteskept.len
      };
      res = hadoop_fuse_do_write(
        path,
        &bufferpos,
        bytesoffset,
        (struct Hadoop_Fuse_FileHandle *) fi->fh,
        NULL, // we don't care about the old locations as we're appending a new block.
        &last);
      free(byteskept.data);
      if(res < 0)
      {
        goto end;
      }
    }

    res = hadoop_fuse_complete(path, fh->fileid, last);
    if(res < 0)
    {
      goto end;
    }
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

enum Hadoop_Fuse_Write_Buffers {
  TRUNCATE = 0,
  NULLPADDING = 1,
  THEDATA = 2,
  TRAILINGDATA = 3
};
#define Hadoop_Fuse_Write_Buffers_Count 4

static
int hadoop_fuse_write(
  const char * src,
  const char * const data,
  const size_t len,
  const off_t offset,
  struct fuse_file_info * fi)
{
  int res;
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto * response = NULL;
  Hadoop__Hdfs__ExtendedBlockProto * last = NULL;
  struct Hadoop_Fuse_FileHandle * fh = (struct Hadoop_Fuse_FileHandle *) fi->fh;
  uint64_t offsetintofile = offset;
  uint64_t lastblockoffset;
  uint32_t n_blocks;

  // at most 3: truncated bytes, the data itself and extract bytes.
  struct Hadoop_Fuse_Buffer buffers[Hadoop_Fuse_Write_Buffers_Count];
  memset(&buffers[0], 0, Hadoop_Fuse_Write_Buffers_Count * sizeof(struct Hadoop_Fuse_Buffer));
  struct Hadoop_Fuse_Buffer_Pos bufferpos = {
    .buffers = &buffers[0],
    .n_buffers = Hadoop_Fuse_Write_Buffers_Count,
    .bufferoffset = 0,
    .len = 0
  };

  request.src = (char *) src;
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
  n_blocks = response->locations->n_blocks;
  lastblockoffset = n_blocks == 0 ? 0 : response->locations->blocks[n_blocks - 1]->offset;

  if(offsetintofile > response->locations->filelength)
  {
    // we should pad the end of the current file with nulls
    buffers[NULLPADDING].len = response->locations->filelength - offsetintofile;
    offsetintofile = response->locations->filelength;
    bufferpos.len += buffers[NULLPADDING].len;
  }

  if(n_blocks > 0 && offsetintofile < lastblockoffset)
  {
    uint64_t truncateto;

    buffers[TRAILINGDATA].len = response->locations->filelength - min(response->locations->filelength, offsetintofile + len);
    bufferpos.len += buffers[TRAILINGDATA].len;

    if(buffers[TRAILINGDATA].len > 0)
    {
#ifndef NDEBUG
      syslog(
        LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
        "hadoop_fuse_write %s stashing %zd bytes to append after write as offset %" PRIu64 " + len %zd < last block offset %" PRIu64 "",
        src,
        buffers[TRAILINGDATA].len,
        offsetintofile,
        len,
        lastblockoffset);
#endif

      buffers[TRAILINGDATA].data = malloc(buffers[TRAILINGDATA].len);
      if(!buffers[TRAILINGDATA].data)
      {
        res = -ENOMEM;
        goto end;
      }
      res = hadoop_fuse_read(
        src,
        buffers[TRAILINGDATA].data,
        buffers[TRAILINGDATA].len,
        offsetintofile + len,
        fi);
      if(res < 0)
      {
        goto end;
      }
    }

    // since we can modify the last block, we only need to throw
    // away blocks (that we've just saved) until we are the last one.
    // However, HDFS semantics mean we can't then overwrite this newly-
    // uncovered last block as it's no longer "under construction". We'll
    // therefore keep the part of that block before our write and drop
    // (and so recreate) the whole thing.

    for(uint32_t b = 0; b < n_blocks; ++b)
    {
      Hadoop__Hdfs__LocatedBlockProto * block = response->locations->blocks[b];
      uint64_t blockend = block->offset + block->b->numbytes;

      if(blockend < offsetintofile)
      {
        truncateto = blockend;
      }
      else
      {
        // we're in the first block we'll truncate
        buffers[TRUNCATE].len = offsetintofile - block->offset;
        bufferpos.len += buffers[TRUNCATE].len;

        if(buffers[TRUNCATE].len > 0)
        {
          buffers[TRUNCATE].data = malloc(buffers[TRUNCATE].len);
          if(!buffers[TRUNCATE].data)
          {
            res = -ENOMEM;
            goto end;
          }
          res = hadoop_fuse_read(
            src,
            buffers[TRUNCATE].data,
            buffers[TRUNCATE].len,
            block->offset,
            fi);
          if(res < 0)
          {
            goto end;
          }
          offsetintofile -= buffers[TRUNCATE].len;
        }

        // keep our response in sync
        n_blocks = b;
        if(b == 0)
        {
          hadoop_fuse_clone_block(NULL, &last);
        }
        else
        {
          hadoop_fuse_clone_block(response->locations->blocks[b - 1]->b, &last);
        }

        break;
      }
    }

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_write %s truncating down to %" PRIu64 " bytes",
      src,
      truncateto);
#endif
    res = hadoop_fuse_ftruncate(src, truncateto, fi);
    if(res < 0)
    {
      goto end;
    }
  }

  res = hadoop_fuse_lock(src, &last);
  if(res < 0)
  {
    goto end;
  }

  buffers[THEDATA].data = (char *) data;
  buffers[THEDATA].len = len;
  bufferpos.len += len;

  res = hadoop_fuse_do_write(
    src,
    &bufferpos,
    offsetintofile,
    fh,
    n_blocks == 0 ? NULL : response->locations->blocks[n_blocks - 1],
    &last);

  // persist the data we've just written, & release the lease we have on "src"
  {
    int complete = hadoop_fuse_complete(src, fh->fileid, last);
    if(res >= 0 && complete < 0)
    {
      res = complete; // use this error if we don't have another
    }
  }

end:
  free(buffers[TRUNCATE].data);
  free(buffers[TRAILINGDATA].data);
  if(response)
  {
    hadoop__hdfs__get_block_locations_response_proto__free_unpacked(response, NULL);
  }
  if(last)
  {
    // these are clones, so we have to free them
    hadoop__hdfs__extended_block_proto__free_unpacked(last, NULL);
  }
  return res < 0 ? res : (int) len;
}

static
int hadoop_fuse_read(const char * src, char * buf, size_t size, off_t offset, struct fuse_file_info * fi)
{
  (void) fi;
  int res;
  Hadoop__Hdfs__GetBlockLocationsRequestProto request = HADOOP__HDFS__GET_BLOCK_LOCATIONS_REQUEST_PROTO__INIT;
  Hadoop__Hdfs__GetBlockLocationsResponseProto * response = NULL;
  Hadoop__Hdfs__ClientOperationHeaderProto clientheader = HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
  Hadoop__Hdfs__BaseHeaderProto baseheader = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
  Hadoop__Hdfs__OpReadBlockProto op = HADOOP__HDFS__OP_READ_BLOCK_PROTO__INIT;

  request.src = (char *) src;
  request.offset = offset;
  request.length = size;
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
  if(response->locations->underconstruction)
  {
    // This means that some (other?) client has the lease on this
    // file. Try again later - maybe they'll have release it?
    res = -EAGAIN;
    goto end;
  }

  clientheader.clientname = hadoop_fuse_client_name();

  for (size_t b = 0; b < response->locations->n_blocks; ++b)
  {
    Hadoop__Hdfs__LocatedBlockProto * block = response->locations->blocks[b];
    Hadoop__Hdfs__BlockOpResponseProto * opresponse = NULL;
    bool read = false;

    if(block->corrupt)
    {
      res = -EBADMSG;
      continue;
    }

    baseheader.block = block->b;
    clientheader.baseheader = &baseheader;
    op.header = &clientheader;
    op.has_sendchecksums = true;
    op.sendchecksums = false;
    op.offset = min(offset - block->offset, 0);     // offset into file -> offset into block
    op.len = min(block->b->numbytes - op.offset, size);

    // for now, don't care about which location we get it from
    for (size_t l = 0; l < block->n_locs && !read; ++l)
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

      read = true;
      hadoop_rpc_disconnect(&dn_state);
    }

#ifndef NDEBUG
    syslog(
      LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
      "hadoop_fuse_read, %s %" PRIu64 " (of %" PRIu64 ") bytes at offset %" PRIu64 " of block %s blk_%" PRIu64 "_%" PRIu64 " => %d",
      (read ? "read" : "NOT read"),
      op.len,
      block->b->numbytes,
      op.offset,
      block->b->poolid,
      block->b->blockid,
      block->b->generationstamp,
      res);
#endif

    if(!read)
    {
      res = res < 0 ? res : -EIO; // use existing error as more specific
      goto end;
    }
  }

  // FUSE semantics, return bytes written on success
  res = size;

end:
#ifndef NDEBUG
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "hadoop_fuse_read, read %zd bytes from %s (offset %zd) => %d",
    size,
    src,
    offset,
    res);
#endif
  hadoop__hdfs__get_block_locations_response_proto__free_unpacked(response, NULL);
  return res;
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

  res = hadoop_rpc_connect_namenode(&state, argv[1], port);
  if(res < 0)
  {
    fprintf(stderr, "connection to hdfs://%s:%d failed: %s\n", argv[1], port, strerror(-res));
    return 1;
  }

#ifndef NDEBUG
  openlog (state.clientname, LOG_CONS | LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
  signal(SIGSEGV, dump_trace);
  syslog(
    LOG_MAKEPRI(LOG_USER, LOG_DEBUG),
    "connected to hdfs://%s:%d, defaults: packetsize=%u, blocksize=%" PRIu64 ", replication=%u, bytesperchecksum=%u, checksumtype=%u",
    argv[1],
    port,
    state.packetsize,
    state.blocksize,
    state.replication,
    state.bytesperchecksum,
    state.checksumtype);
#endif

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

