#ifndef _EDGEFS_EDGEFS_H
#define _EDGEFS_EDGEFS_H

#include <vector>
#include <list>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <cstring>
#include <unordered_map>
#include <cstdint>
#include <ctime>

#include <stdlib.h>
#include <fuse.h>

#include "edgefs/edgerpc.h"
#include "edgefs/option.h"

namespace edgefs
{

class BitMap;

enum class chunck_state {
  ALIVE,
  WAIT4GC,
  INVAILD
};

enum class GC_REASON {
  EXPIRED,
  COLDDATA
};

struct dentry {
  std::string     d_name;
  struct inode*   d_inode;
  struct dentry*  d_parent;
  std::map<std::string, struct dentry*> d_childs;
};

struct inode {
  uint64_t  i_len;           // file length
  uint64_t  i_chunck_size;   // Disk chunck: 64 MB default (64 * 1024 * 1024)
  uint64_t  i_block_size;    // Memory block: 2 MB default (2 * 1024 * 1024)
  uint64_t  i_nlink;         // hard link number
  uint64_t  i_lock;          // file lock
  time_t    i_mtime;         // last modify time
  time_t    i_atime;         // last access time
  mode_t    i_mode;          // inode mode
  uid_t     i_uid;           // user id
  gid_t     i_gid;           // group id

  BitMap*   i_shard_bitmap;  // if chunck exist 
  std::map<uint64_t, subinode*> i_subinodes;  // chunck file inode
};

struct subinode {
  uint64_t      subi_no;                              // chunck no
  inode*        subi_inode;                           // parent inode
  chunck_state  subi_state;                           // chunck state
  time_t        subi_ctime;                           // last modify time
  time_t        subi_atime;                           // last access time
  uint64_t      subi_acounter;                        // total access times

  BitMap*       subi_block_bitmap;                    // if cache block exist
  std::map<uint64_t, struct cacheblock*> subi_blocks;  // cache blocks
};

struct file {
  struct dentry* f_dentry;
  std::atomic<uint32_t> f_ref;
};

struct pull_request {
  std::string pr_path;        // file path
  uint64_t pr_chunck_size;    // file chunck size
  uint64_t pr_start_chunck;   // first chunck no 
  std::size_t pr_chunck_num;  // chuncks number
};

struct gc_request {
  std::string gcr_path;         // file path
  uint64_t gcr_start_chunck_no; // gc start chunck number
  uint64_t gcr_chuncks_num;     // gc chuncks number
  GC_REASON gcr_reason;         // gc reason
};

class EdgeFS {
public:
  static void Init(std::string config_path);
  static int getattr(const char *path, struct stat *st);
  static int mknod(const char *path, mode_t mode, dev_t);
  static int mkdir(const char *path, mode_t mode);
  static int rmdir(const char *path);
  static int rename(const char *path1, const char *path2, unsigned int flags);
  static int read(const char *path, char *buf, std::size_t size, off_t off, struct fuse_file_info *);
  static int write(const char *path, const char *data, std::size_t size, off_t off, struct fuse_file_info *);
  // static int statfs(const char *, struct statvfs *);
  static int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info * fi);
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

  static struct dentry* root_dentry_;     // fs root dentry

  static std::list<gc_request*> gc_list_;
  static std::mutex gc_mtx_;
  static std::condition_variable gc_cv_;
  
  static std::list<pull_request*> pull_list_;
  static std::mutex pull_mtx_;
  static std::condition_variable pull_cv_;

  static Option options_;

  friend class EdgeServiceImpl;
};

} // namespace edgefs


#endif