// #include <thread>
#include <iostream>
#include <string>
#include <cstring>

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
  char* new_chars = new char[20];
  memset(new_chars, 0, 20);
  new_chars[0] = 5;
  new_chars[1] = 5;
  std::string s(new_chars, 6);
  std::cout << s.size() << std::endl;
  return 0;
}