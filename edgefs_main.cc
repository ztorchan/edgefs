#define FUSE_USE_VERSION 29

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fuse.h>

#include <iostream>
#include <glog/logging.h>

#include "edgefs/edgefs.h"

static const struct fuse_operations edgefs_ops = {
  .getattr = edgefs::EdgeFS::getattr,
  .read = edgefs::EdgeFS::read,
  .readdir = edgefs::EdgeFS::readdir
};

int main(int argc, char* argv[]) {
  edgefs::EdgeFS::Init("./default_config.json");
  return fuse_main(argc, argv, &edgefs_ops, NULL);
}