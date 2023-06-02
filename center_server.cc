#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include "edgefs/centerrpc.h"

DEFINE_string(data_path, "/", "Basic path to find data");

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  brpc::FLAGS_max_body_size = 256 * 1024 * 1024;

  brpc::Server center_server;
  edgefs::CenterServiceImpl center_service(FLAGS_data_path);
  if(center_server.AddService(&center_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    spdlog::error("[main] failed to start center service");
    exit(-1);
  }

  butil::EndPoint listen_ep(butil::IP_ANY, 2333);
  brpc::ServerOptions options;
  if(center_server.Start(listen_ep, &options) != 0) {
    spdlog::error("[main] fail to start centerRPC");
    exit(-1);
  } else {
    spdlog::info("[main] centerrpc start");
  }
  center_server.RunUntilAskedToQuit();
  spdlog::info("[main] centerrpc stop");
}