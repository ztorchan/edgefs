#include <thread>
#include <iostream>

void T() {
  for(int i = 0; i < 100000; i++) {
    std::cout << i << std::endl;
  }
}

class TestClass {
public:
  static std::thread* t_ptr;

  static void Init() {
    t_ptr = new std::thread(T);
  }

  static void End() {
    if(t_ptr->joinable()) {
      t_ptr->join();
    }
    delete t_ptr;
  }
};

std::thread* TestClass::t_ptr;

int main() {
  TestClass::Init();

  for(char c = 'a'; c < 'z'; c++) {
    std::cout << c << std::endl;
  }

  TestClass::End();

  return 0;
}