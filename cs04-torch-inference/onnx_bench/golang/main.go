package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	ort "github.com/yalue/onnxruntime_go"
)

func readBin(path string) ([]float32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	floats := make([]float32, len(data)/4)
	for i := 0; i < len(floats); i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		floats[i] = math.Float32frombits(bits)
	}
	return floats, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <model_type>\n", os.Args[0])
		return
	}
	modelType := os.Args[1]
	modelPath := fmt.Sprintf("data/%s.onnx", modelType)
	inputPath := fmt.Sprintf("data/example_input_single_%s.bin", modelType)

	// Adjust library path based on OS if needed. Defaulting to Linux naming.
	libPath := os.Getenv("ONNXRUNTIME_LIB_PATH")
	if libPath == "" {
		libPath = "libonnxruntime.so"
	}
	ort.SetSharedLibraryPath(libPath)
	err := ort.Initialize()
	if err != nil {
		panic(err)
	}
	defer ort.Destroy()

	inputData, err := readBin(inputPath)
	if err != nil {
		panic(err)
	}

	var inputShape ort.Shape
	if modelType == "simple" {
		inputShape = ort.Shape{1, 128}
	} else if modelType == "rnn" {
		inputShape = ort.Shape{1, 10, 32}
	} else if modelType == "transformer" {
		inputShape = ort.Shape{1, 16, 64}
	}

	inputTensor, err := ort.NewTensor(inputShape, inputData)
	if err != nil {
		panic(err)
	}
	defer inputTensor.Destroy()

	outputData := make([]float32, 10)
	outputShape := ort.Shape{1, 10}
	outputTensor, err := ort.NewTensor(outputShape, outputData)
	if err != nil {
		panic(err)
	}
	defer outputTensor.Destroy()

	session, err := ort.NewAdvancedSession(modelPath,
		[]string{"x"}, []string{"output"},
		[]ort.ArbitraryTensor{inputTensor}, []ort.ArbitraryTensor{outputTensor}, nil)
	if err != nil {
		panic(err)
	}
	defer session.Destroy()

	// Warmup
	err = session.Run()
	if err != nil {
		panic(err)
	}

	var latencies []float64
	for i := 0; i < 20; i++ {
		start := time.Now()
		err = session.Run()
		if err != nil {
			panic(err)
		}
		latencies = append(latencies, float64(time.Since(start).Nanoseconds())/1e6)
	}

	sort.Float64s(latencies)
	p95 := latencies[int(float64(len(latencies))*0.95)]
	fmt.Printf("Single inference P95 (Go ONNX): %.4f ms\n", p95)
}
