#ifndef _EDGEFS_EDGEFS_H
#define _EDGEFS_EDGEFS_H

#define FUSE_USE_VERSION 39

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
#include <fuse3/fuse.h>
#include <brpc/channel.h>

#include "edgefs/mm.h"
#include "edgefs/edgerpc.h"
#include "edgefs/option.h"

#define _OUT

namespace edgefs
{

class BitMap;

/* Some State and Type enum */
enum class FileType {
  REGULAR,
  DIRECTORY,
};

enum class FileState {
  ALIVE,
  INVALID
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
struct edgefs_dentry;
struct edgefs_inode;
struct edgefs_subinode;

struct edgefs_dentry {
  std::string         d_name;
  FileType            d_type;
  edgefs_inode*       d_inode;        // nullptr if directory
  edgefs_dentry*      d_parent;
  uint64_t    d_ref;
  std::shared_mutex   d_mtx;
  std::map<std::string, edgefs_dentry*>* d_childs; // nullptr if regular file
};

struct edgefs_inode {
  uint64_t        i_len;           // file length
  uint64_t        i_chunck_size;   // Disk chunck: 64 MB default (64 * 1024 * 1024)
  uint64_t        i_block_size;    // Memory block: 2 MB default (2 * 1024 * 1024)
  FileState       i_state;         // file state
  time_t          i_mtime;         // last modify time
  time_t          i_atime;         // last access time
  mode_t          i_mode;          // inode mode
  
  edgefs_dentry*  i_dentry;

  BitMap*         i_chunck_bitmap; // if chunck exist 
  std::map<uint64_t, edgefs_subinode*> i_subinodes;  // chunck file inode
};

struct edgefs_subinode {
  uint64_t              subi_no;                // chunck no
  edgefs_inode*         subi_inode;             // parent inode
  time_t                subi_ctime;             // last modify time
  time_t                subi_atime;             // last access time
  uint64_t              subi_acounter;          // total access times
  std::shared_mutex     subi_mtx;               // mutex to protect blocks
  ChunckState           subi_state;             // chunck state

  BitMap* subi_block_bitmap;                    // if cache block exist
  std::map<uint64_t, cacheblock*> subi_blocks;  // cache blocks
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
  static int getattr(const char *path, struct stat *st, struct fuse_file_info *fi);
  static int read(const char *path, char *buf, std::size_t size, off_t off, struct fuse_file_info *);
  static int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info * fi, enum fuse_readdir_flags flags);

private:
  static void split_path(const char *path, std::vector<std::string>& d_names);
  static std::string get_path_from_dentry(edgefs_dentry* den);
  static int read_from_chunck(const char *path, edgefs_subinode* subi, char *buf, std::size_t size, off_t offset);
  static bool check_chuncks_exist(edgefs_inode* in, uint64_t start_chunck_no, uint64_t end_chunck_no, _OUT std::vector<std::pair<uint64_t, uint64_t>>& lack_extent);
  static void gc_extent(edgefs_inode* in, uint64_t start_chunck_no, uint64_t chunck_num);
  static void gc_whole_file(edgefs_inode* in);
  static void dfs_scan(edgefs_dentry* cur_den);
  static void RPC();
  static void GC_AND_PULL();
  static void SCAN();

private:
  static std::thread* rpc_thread_;
  static std::thread* gc_and_pull_thread_;
  static std::thread* scan_thread_;

  static edgefs_dentry* root_dentry_;     // fs root dentry

  static std::list<request*> req_list_;
  static std::mutex req_mtx_;
  static std::condition_variable req_cv_;

  static Option options_;
  static MManger* mm_;
  static brpc::Channel center_chan_;

  friend class EdgeServiceImpl;
  friend class EdgeChunckStreamReceiver;
};

} // namespace edgefs


#endif