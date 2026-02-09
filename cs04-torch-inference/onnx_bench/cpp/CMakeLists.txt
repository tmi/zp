cmake_minimum_required(VERSION 3.10)
project(onnx_bench_cpp)

set(CMAKE_CXX_STANDARD 17)

find_package(onnxruntime REQUIRED)

add_executable(onnx_bench main.cpp)
target_link_libraries(onnx_bench onnxruntime)
