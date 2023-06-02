#define FUSE_USE_VERSION 39

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#include <gflags/gflags.h>

#include "edgefs/edgefs.h"

DEFINE_string(mount_point, "", "Mount point");

static const struct fuse_operations edgefs_ops = {
  .getattr = edgefs::EdgeFS::getattr,
  .read = edgefs::EdgeFS::read,
  .readdir = edgefs::EdgeFS::readdir
};

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  char mount_point[256];
  memset(mount_point, 0, 256);
  memcpy(mount_point, FLAGS_mount_point.c_str(), 256);

  edgefs::EdgeFS::Init("./default_config.json");
  char* r_argv[] = {"hybridfs", "-f", mount_point};
  int fuse_state = fuse_main(3, r_argv, &edgefs_ops, NULL);
  return fuse_state;
}