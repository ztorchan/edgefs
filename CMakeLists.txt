cmake_minimum_required(VERSION 3.16.0)
project(utorfs C CXX)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -D_FILE_OFFSET_BITS=64")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
if(NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX /usr/local)
endif()

include(FindProtobuf)
file(GLOB_RECURSE PROTO_FILE_LIST ${CMAKE_SOURCE_DIR}/protos/*.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/)
foreach(PROTO_FILE ${PROTO_FILE_LIST})
  get_filename_component(FILE_NAME ${PROTO_FILE} NAME_WE)
  list(APPEND PROTO_CXX "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.cc")
  list(APPEND PROTO_H "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.h")
  execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR}/protos/ --cpp_out=${PROTO_OUT_DIR} ${PROTO_FILE}
  )
endforeach()

include_directories(${CMAKE_SOURCE_DIR}/include/)

find_package(gflags REQUIRED)
find_package(OpenSSL)
find_package(Threads)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_library(LEVELDB_LIB NAMES leveldb)

set(SRCS
  ${CMAKE_SOURCE_DIR}/edgefs.cc
)

set(LIBS 
  ${BRPC_LIB}
  ${PROTOBUF_LIBRARIES}
  ${OPENSSL_SSL_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${LEVELDB_LIB}
  ${CMAKE_THREAD_LIBS_INIT}
  gflags
  dl
  stdc++fs
  fuse
)

add_executable(edgefs ${SRCS})
target_link_libraries(edgefs ${LIBS})