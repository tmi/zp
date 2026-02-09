#include <iostream>
#include <vector>
#include <chrono>
#include <fstream>
#include <numeric>
#include <algorithm>
#include <onnxruntime_cxx_api.h>

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
        std::cerr << "Usage: " << argv[0] << " <model_type> [device]" << std::endl;
        return 1;
    }
    std::string model_type = argv[1];
    std::string device = (argc > 2) ? argv[2] : "cpu";

    std::string model_path = "data/" + model_type + ".onnx";
    std::string single_input_path = "data/example_input_single_" + model_type + ".bin";
    std::string batch_input_path = "data/example_input_batch_" + model_type + ".bin";

    try {
        Ort::Env env(ORT_LOGGING_LEVEL_WARNING, "ONNX_Bench");
        Ort::SessionOptions session_options;

        if (device == "cuda") {
            // OrtSessionOptionsAppendExecutionProvider_CUDA(session_options, 0);
        }

        Ort::Session session(env, model_path.c_str(), session_options);
        Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

        auto input_data = read_bin(single_input_path);

        // This is a simplified example, real code would need to inspect model for input shapes
        // and names properly.
        const char* input_names[] = {"x"};
        const char* output_names[] = {"output"};

        // Single inference
        std::vector<int64_t> input_shape; // Need to set this based on model
        if (model_type == "simple") input_shape = {1, 128};
        else if (model_type == "rnn") input_shape = {1, 10, 32};
        else if (model_type == "transformer") input_shape = {1, 16, 64};

        auto input_tensor = Ort::Value::CreateTensor<float>(memory_info, input_data.data(), input_data.size(), input_shape.data(), input_shape.size());

        // Warmup
        session.Run(Ort::RunOptions{nullptr}, input_names, &input_tensor, 1, output_names, 1);

        std::vector<double> latencies;
        for (int i = 0; i < 20; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            session.Run(Ort::RunOptions{nullptr}, input_names, &input_tensor, 1, output_names, 1);
            auto end = std::chrono::high_resolution_clock::now();
            latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());
        }

        std::sort(latencies.begin(), latencies.end());
        double p95 = latencies[static_cast<int>(latencies.size() * 0.95)];
        std::cout << "Single inference P95 (C++ ONNX): " << p95 << " ms" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
