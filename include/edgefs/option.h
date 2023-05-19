#ifndef _EDGEFS_OPTION_H
#define _EDGEFS_OPTION_H

#include <cstdlib>
#include <string>
#include <fstream>
#include <sstream>

#include <butil/logging.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

namespace edgefs
{
  
class Option {
public:
  Option() {}
  Option(std::string config_path) {
    std::ifstream ifs(config_path);
    std::stringstream buf;
    buf << ifs.rdbuf();
    std::string config_json(buf.str());
    ifs.close();

    rapidjson::Document doc;
    doc.Parse(config_json.c_str());
    assert(doc.IsObject());

    /* Necessary config */
    // Data Center address
    assert(doc.HasMember("center_address"));
    assert(doc["center_address"].IsString());
    center_address = doc["center_address"].GetString();

    // Data Center rpc port
    assert(doc.HasMember("center_port"));
    assert(doc["center_port"].IsUint());
    center_port = doc["center_port"].GetUint();

    /* Optional config */
    // root path
    if(doc.HasMember("root")) {
      assert(doc["root"].IsString());
      root_path = doc["root"].GetString();
      if(root_path[root_path.size() - 1] == '/') {
        root_path.pop_back();
      }
    }

    // data root path
    if(doc.HasMember("data_root")) {
      assert(doc["data_root"].IsString());
      data_root_path = doc["data_root"].GetString();
      if(data_root_path[root_path.size() - 1] == '/') {
        data_root_path.pop_back();
      }
    }

    // edge rpc listen port
    if(doc.HasMember("rpc_port")) {
      assert(doc["rpc_port"].IsUint());
      rpc_port = doc["rpc_port"].GetUint();
    }

    // file chunck size
    if(doc.HasMember("chunck_size")) {
      assert(doc["chunck_size"].IsUint64());
      chunck_size = doc["chunck_size"].GetUint64();
    } 

    // file cache block size 
    if(doc.HasMember("block_size")) {
      assert(doc["block_size"].IsUint64());
      block_size = doc["block_size"].GetUint64();
    }
    if(chunck_size < block_size) {
      LOG(ERROR) << "Chunk size should be greater than block size";
      exit(-1);
    }
    if(chunck_size % block_size != 0) {
      LOG(ERROR) << "Chunk size should be divisible by block size";
      exit(-1);
    }

    // extra forward chuncks nums when pull
    if(doc.HasMember("forward_pre_pull_chunck_num")) {
      assert(doc["forward_pre_pull_chunck_num"].IsUint64());
      forward_pre_pull_chunck_num = doc["forward_pre_pull_chunck_num"].GetUint64();
    }

    // extra backward chuncks nums when pull
    if(doc.HasMember("backward_pre_pull_chunck_num")) {
      assert(doc["backward_pre_pull_chunck_num"].IsUint64());
      backward_pre_pull_chunck_num = doc["backward_pre_pull_chunck_num"].GetUint64();
    }

    // max size of total block cache
    if(doc.HasMember("max_cache_size")) {
      assert(doc["max_cache_size"].IsUint());
      max_cache_size = doc["max_cache_size"].GetUint();
    }

    // max size of total free cache hold by mm
    if(doc.HasMember("max_free_cache")) {
      assert(doc["max_free_cache"].IsUint64());
      max_free_cache = doc["max_free_cache"].GetUint64();
    }
    if(max_cache_size < max_free_cache) {
      LOG(ERROR) << "max_cache_size should be greater than max_free_cache";
      exit(-1);
    }

    // Minimum active time after Chunck is accessed
    if(doc.HasMember("min_chunck_active_time")) {
      assert(doc["min_chunck_active_time"].IsUint());
      min_chunck_active_time = doc["min_chunck_active_time"].GetUint();
    }

    // Maximum avg access period required to keep active
    if(doc.HasMember("chunck_active_access_frequency")) {
      assert(doc["chunck_active_access_frequency"].IsUint());
      chunck_active_access_frequency = doc["chunck_active_access_frequency"].GetUint();
    }

    // Minimum active time after Chunck is accessed
    if(doc.HasMember("min_block_active_time")) {
      assert(doc["min_block_active_time"].IsUint());
      min_block_active_time = doc["min_block_active_time"].GetUint();
    }

    // Maximum avg access period required to keep active
    if(doc.HasMember("block_active_access_frequency")) {
      assert(doc["block_active_access_frequency"].IsUint());
      block_active_access_frequency = doc["block_active_access_frequency"].GetUint();
    }

    // Scan thread work period
    if(doc.HasMember("scan_period")) {
      assert(doc["scan_period"].IsUint());
      scan_period = doc["scan_period"].GetUint();
    }
  }

public:
  // Root path of EdgeFS (Metadata)
  std::string root_path = "/edge";

  // Root path of EdgeFS data (Data)
  std::string data_root_path = "/edgedata";

  // Data center address(ipv4)
  std::string center_address;

  // Data center rpc port
  uint32_t center_port;

  // Edge RPC listen port
  uint32_t rpc_port = 2333;

  // File chunck size (bytes)
  uint64_t chunck_size = 64 * 1024 * 1024;

  // File cache block size (bytes)
  uint64_t block_size = 8 * 1024 * 1024;

  // Forward prepull chuncks num
  uint64_t forward_pre_pull_chunck_num = 4;

  // Backward prepull chuncks num
  uint64_t backward_pre_pull_chunck_num = 2;

  // Max size of total block cache
  uint32_t max_cache_size = 1024 * 1024 * 1024;

  // Max size of total free cache hold by mm
  uint32_t max_free_cache = 128 * 1024 * 1024;

  // Minimum active time after chunck is accessed
  uint32_t min_chunck_active_time = 60;

  // Maximum avg access period required to keep chunck active
  uint32_t chunck_active_access_frequency = 30;

  // Minimum active time after block is accessed
  uint32_t min_block_active_time = 20;

  // Maximum avg access period required to keep block active
  uint32_t block_active_access_frequency = 10;

  // Scan thread work period
  uint32_t scan_period = 60;
};

} // namespace edgefs


#endif