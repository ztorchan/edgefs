#define FUSE_USE_VERSION 29

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fuse.h>

#include <iostream>
#include <glog/logging.h>

static int edgefs_getattr(const char* path, struct stat* st) {
  memset(st, 0, sizeof(struct stat));
  if(strcmp(path, "/") == 0) {
    st->st_mode = 0755 | __S_IFDIR;
  } else {
    st->st_mode = 0644 | __S_IFREG;
  }
  return 0;
}

static struct fuse_operations edgefs_ops = {
  .getattr = edgefs_getattr,
};

int main(int argc, char* argv[]) {
  std::cout << "Mount" << std::endl;
  return fuse_main(argc, argv, &edgefs_ops, NULL);
}