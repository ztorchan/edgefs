#ifndef _EDGEFS_EDGEFS_H
#define _EDGEFS_EDGEFS_H

#include <vector>
#include <list>
#include <map>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <cstring>
#include <unordered_map>
#include <cstdint>
#include <ctime>

#include <stdlib.h>
#include <fuse.h>

#include "edgefs/mm.h"
#include "edgefs/edgerpc.h"
#include "edgefs/option.h"

#define _OUT

namespace edgefs
{

class BitMap;

/* Some State and Type enum */
enum class FileState {
  ALIVE,
  INVAILD
};

enum class ChunckState {
  ACTIVE,
  INACTIVE
};

enum class RequestType {
  PULL,
  GC,
};

enum class GCReason {
  INACTIVECHUNCK,
  INVAILDFILE
};

/**/

/* File system normal struct */
struct dentry {
  std::string     d_name;
  inode*   d_inode;        // nullptr means directory
  dentry*  d_parent;
  uint64_t        d_ref;
  std::shared_mutex      d_mtx;
  std::map<std::string, struct dentry*> d_childs;
};

struct inode {
  uint64_t    i_len;           // file length
  uint64_t    i_chunck_size;   // Disk chunck: 64 MB default (64 * 1024 * 1024)
  uint64_t    i_block_size;    // Memory block: 2 MB default (2 * 1024 * 1024)
  FileState   i_state;         // file state
  time_t      i_mtime;         // last modify time
  time_t      i_atime;         // last access time
  mode_t      i_mode;          // inode mode
  
  dentry* i_dentry;

  BitMap*     i_chunck_bitmap; // if chunck exist 
  std::map<uint64_t, subinode*> i_subinodes;  // chunck file inode
};

struct subinode {
  uint64_t      subi_no;                              // chunck no
  inode*        subi_inode;                           // parent inode
  time_t        subi_ctime;                           // last modify time
  time_t        subi_atime;                           // last access time
  uint64_t      subi_acounter;                        // total access times
  ChunckState   subi_state;

  BitMap*       subi_block_bitmap;                    // if cache block exist
  std::map<uint64_t, cacheblock*> subi_blocks; // cache blocks
};
/**/

/* Request */
struct request {
  RequestType r_type;         // request type
  time_t      r_time;         // request time
};

struct pull_request : public request {
  std::string pr_path;        // file path
  uint64_t pr_chunck_size;    // file chunck size
  uint64_t pr_start_chunck;   // first chunck no 
  uint64_t pr_chunck_num;     // chuncks number
};

struct gc_request : public request {
  std::string gcr_path;         // file path
  uint64_t gcr_start_chunck_no; // gc start chunck number
  uint64_t gcr_chuncks_num;     // gc chuncks number
  GCReason gcr_reason;          // gc reason
};
/**/


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
  static std::string get_path_from_inode(inode* in);
  static int read_from_chunck(const char *path, subinode* subi, char *buf, std::size_t size, off_t offset);
  static bool check_chuncks_exist(inode* in, uint64_t start_chunck_no, uint64_t end_chunck_no, _OUT std::vector<std::pair<uint64_t, uint64_t>> lack_extent);
  static void gc_extent(inode* in, uint64_t start_chunck_no, uint64_t chunck_num);
  static void gc_whole_file(inode* in);
  static void dfs_scan(dentry* cur_den);
  static void RPC();
  static void GC_AND_PULL();
  static void SCAN();

private:
  static std::thread* rpc_thread_;
  static std::thread* gc_and_pull_thread_;
  static std::thread* scan_thread_;

  static dentry* root_dentry_;     // fs root dentry

  static std::list<request*> req_list_;
  static std::mutex req_mtx_;
  static std::condition_variable req_cv_;

  static Option options_;
  static MManger* mm_;

  friend class EdgeServiceImpl;
};

} // namespace edgefs


#endif