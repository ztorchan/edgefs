#ifndef _EDGEFS_EDGEFS_H
#define _EDGEFS_EDGEFS_H

#include <vector>
#include <list>
#include <map>
#include <thread>
#include <mutex>
#include <atomic>
#include <string>
#include <cstring>
#include <unordered_map>
#include <cstdint>

#include <stdlib.h>
#include <fuse.h>

#include "edgefs/edgerpc.h"

namespace edgefs
{

class BitMap;

enum class chunck_state {
  ALIVE,
  INVAILD
};

struct dentry {
  std::string d_name;
  struct inode* d_inode;
  struct dentry* d_parent;
  std::map<std::string, struct dentry*> d_childs;
};

struct inode {
  uint64_t i_len;           // file length
  uint64_t i_chunck_size;   // Disk chunck: 64 MB default (64 * 1024 * 1024)
  uint64_t i_block_size;    // Memory block: 2 MB default (2 * 1024 * 1024)
  uint64_t i_nlink;         // hard link number
  uint64_t i_lock;          // file lock
  time_t i_mtime;           // last modify time
  time_t i_atime;           // last access time
  mode_t i_mode;            // inode mode
  uid_t i_uid;
  gid_t i_gid;

  BitMap* i_shard_bitmap;   // if chunck exist 
  std::map<uint64_t, subinode*> i_subinodes;  // chunck file inode
};

struct subinode {
  inode* subi_inode;        // parent inode
  uint64_t subi_no;         // chunck no
  chunck_state subi_state;  // chunck state
  BitMap* subi_block_bitmap;// if memcache block exist

  time_t subi_atime;        // last access time
  uint64_t subi_rcounter;   // total access times
};

struct file {
  struct dentry* f_dentry;
  std::atomic<uint32_t> f_ref;
};

class EdgeFS {
public:
  static void Init();
  static int getattr(const char *path, struct stat *st);
  static int mknod(const char *path, mode_t mode, dev_t);
  static int mkdir(const char *path, mode_t mode);
  static int rmdir(const char *path);
  static int rename(const char *path1, const char *path2, unsigned int flags);
  static int read(const char *path, char *buf, std::size_t size, off_t off, struct fuse_file_info *);
  static int write(const char *path, const char *data, std::size_t size, off_t off, struct fuse_file_info *);
  // static int statfs(const char *, struct statvfs *);
  static int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offser, struct fuse_file_info * fi);
  static int open(const char *path, struct fuse_file_info *fi);
  static int unlink(const char *path);
  static int releasedir(const char *path, struct fuse_file_info *fi);

private:
  static void RPC();
  static void GC();
  static void PULL();
  static void SCAN();

private:
  static std::thread* rpc_thread_;
  static std::thread* gc_thread_;
  static std::thread* pull_thread_;
  static std::thread* scan_thread_;

  static std::string center_ipv4_;
  static struct dentry* root_dentry_;

  friend class EdgeServiceImpl;
};

} // namespace edgefs


#endif