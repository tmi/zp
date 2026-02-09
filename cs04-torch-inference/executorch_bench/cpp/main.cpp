#include <executorch/runtime/core/error.h>
#include <executorch/runtime/executor/method.h>
#include <executorch/runtime/executor/program.h>
#include <executorch/extension/data_loader/file_data_loader.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <fstream>
#include <numeric>
#include <algorithm>

using namespace executorch::runtime;
using namespace executorch::extension;

std::vector<float> read_bin(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Could not open file: " + path);
    }
    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<float> buffer(size / sizeof(float));
    if (!file.read(reinterpret_cast<char*>(buffer.data()), size)) {
        throw std::runtime_error("Error reading file: " + path);
    }
    return buffer;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <model_type>" << std::endl;
        return 1;
    }
    std::string model_type = argv[1];
    std::string model_path = "data/" + model_type + "_single.pte";
    std::string input_path = "data/example_input_single_" + model_type + ".bin";

    // Load the program
    Result<FileDataLoader> loader = FileDataLoader::from(model_path.c_str());
    if (!loader.ok()) {
        std::cerr << "Failed to create loader" << std::endl;
        return 1;
    }

    Result<Program> program = Program::load(&loader.get());
    if (!program.ok()) {
        std::cerr << "Failed to load program" << std::endl;
        return 1;
    }

    // Load the method
    Result<Method> method = program->load_method("forward");
    if (!method.ok()) {
        std::cerr << "Failed to load method" << std::endl;
        return 1;
    }

    // Load input data
    std::vector<float> input_data;
    try {
        input_data = read_bin(input_path);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    // Set input (Simplified, assuming first input is the one we want)
    // In real ExecuTorch, setting inputs can be more involved depending on memory planning.
    // method->set_input(tensor, 0);

    // Warmup
    Error error = method->execute();
    if (error != Error::Ok) {
        std::cerr << "Execution failed" << std::endl;
        return 1;
    }

    std::vector<double> latencies;
    for (int i = 0; i < 20; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        method->execute();
        auto end = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    }

    std::sort(latencies.begin(), latencies.end());
    double p95 = latencies[static_cast<int>(latencies.size() * 0.95)];
    std::cout << "Single inference P95 (C++ ExecuTorch): " << p95 << " ms" << std::endl;

    return 0;
}
