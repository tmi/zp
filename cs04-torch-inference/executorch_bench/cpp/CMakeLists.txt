cmake_minimum_required(VERSION 3.10)
project(executorch_bench_cpp)

set(CMAKE_CXX_STANDARD 17)

# Assuming ExecuTorch is installed or built in a specific path
find_package(executorch REQUIRED)

add_executable(executorch_bench main.cpp)
target_link_libraries(executorch_bench executorch)
