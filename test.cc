// #include <thread>
#include <iostream>
#include <string>
#include <cstring>
#include <butil/iobuf.h>

// void T() {
//   for(int i = 0; i < 100000; i++) {
//     std::cout << i << std::endl;
//   }
// }

// class TestClass {
// public:
//   static std::thread* t_ptr;

//   static void Init() {
//     t_ptr = new std::thread(T);
//   }

//   static void End() {
//     if(t_ptr->joinable()) {
//       t_ptr->join();
//     }
//     delete t_ptr;
//   }
// };

// std::thread* TestClass::t_ptr;

// int main() {
//   TestClass::Init();

//   for(char c = 'a'; c < 'z'; c++) {
//     std::cout << c << std::endl;
//   }

//   TestClass::End();

//   return 0;
// }

int main() {
  butil::IOBuf iobuf;
  uint64_t a = 64;
  std::string a_path = "/home/ubuntu/aaa";
  iobuf.append(&a, sizeof(uint64_t));
  iobuf.append(a_path);

  uint64_t b = UINT64_MAX;
  iobuf.cutn(&b, sizeof(uint64_t));
  std::string b_path = iobuf.to_string();

  std::cout << b << std::endl;
  std::cout << b_path << std::endl;
}