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

        const char* input_names[] = {"x"};
        const char* output_names[] = {"output"};

        // Single inference
        auto single_input_data = read_bin(single_input_path);
        std::vector<int64_t> single_input_shape;
        if (model_type == "simple") single_input_shape = {1, 128};
        else if (model_type == "rnn") single_input_shape = {1, 10, 32};
        else if (model_type == "transformer") single_input_shape = {1, 16, 64};

        auto single_input_tensor = Ort::Value::CreateTensor<float>(memory_info, single_input_data.data(), single_input_data.size(), single_input_shape.data(), single_input_shape.size());

        // Warmup
        session.Run(Ort::RunOptions{nullptr}, input_names, &single_input_tensor, 1, output_names, 1);

        std::vector<double> latencies;
        for (int i = 0; i < 20; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            session.Run(Ort::RunOptions{nullptr}, input_names, &single_input_tensor, 1, output_names, 1);
            auto end = std::chrono::high_resolution_clock::now();
            latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());
        }

        std::sort(latencies.begin(), latencies.end());
        double p95 = latencies[static_cast<int>(latencies.size() * 0.95)];
        std::cout << "Single inference P95 (C++ ONNX): " << p95 << " ms" << std::endl;

        // Batch inference
        auto batch_input_data = read_bin(batch_input_path);
        std::vector<int64_t> batch_input_shape;
        if (model_type == "simple") batch_input_shape = {20, 128};
        else if (model_type == "rnn") batch_input_shape = {20, 10, 32};
        else if (model_type == "transformer") batch_input_shape = {20, 16, 64};

        auto batch_input_tensor = Ort::Value::CreateTensor<float>(memory_info, batch_input_data.data(), batch_input_data.size(), batch_input_shape.data(), batch_input_shape.size());

        // Warmup
        session.Run(Ort::RunOptions{nullptr}, input_names, &batch_input_tensor, 1, output_names, 1);

        std::vector<double> batch_latencies;
        for (int i = 0; i < 3; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            session.Run(Ort::RunOptions{nullptr}, input_names, &batch_input_tensor, 1, output_names, 1);
            auto end = std::chrono::high_resolution_clock::now();
            batch_latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());
        }

        double sum = std::accumulate(batch_latencies.begin(), batch_latencies.end(), 0.0);
        double mean_batch = sum / batch_latencies.size();
        std::cout << "Batch inference mean (C++ ONNX): " << mean_batch << " ms" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
