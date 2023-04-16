#ifndef _EDGEFS_EDGEFS_H
#define _EDGEFS_EDGEFS_H

#include <vector>
#include <thread>
#include <string>
#include <cstring>
#include <unordered_map>
#include <cstdint>

#include <fuse.h>

namespace edgefs
{

enum class dentry_type {
  NORMAL,
  DIRECTORY
};

struct dentry {
  std::string d_name;
  dentry_type d_type;
  struct inode* d_inode;
  struct dentry* d_parent;
  std::unordered_map<std::string, struct dentry*> d_childs;
};

struct inode {
  uint64_t i_blocksize;
  std::vector<char*> i_memblocks;
  
};

struct file {
  struct dentry* f_dentry;
  uint32_t f_ref;
};

class EdgeFS {
public:
  static void Init();
  static int getattr(const char *path, struct stat *st, struct fuse_file_info *fi);
  static int mknod(const char *path, mode_t mode, dev_t);
  static int mkdir(const char *path, mode_t mode);
  static int rmdir(const char *path);
  static int rename(const char *path1, const char *path2, unsigned int flags);
  static int read(const char *path, char *buf, std::size_t size, off_t off, struct fuse_file_info *);
  static int write(const char *path, const char *data, std::size_t size, off_t off, struct fuse_file_info *);
  static int statfs(const char *, struct statvfs *);
  static int readdir(
      const char *path, void *buf, fuse_fill_dir_t filler, off_t, struct fuse_file_info *, enum fuse_readdir_flags);
  static int open(const char *path, struct fuse_file_info *fi);
  static int unlink(const char *path);
  static int releasedir(const char *path, struct fuse_file_info *fi);

private:
  static std::thread* rpc_server_;
  static struct dentry* root_dentry_;

  
};

} // namespace edgefs


#endif