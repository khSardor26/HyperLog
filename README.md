# HyperLog 🚀

A high-throughput, multi-threaded log processing and analytics engine built in **.NET 10** using a Producer-Consumer architecture (`System.Threading.Channels` + `System.IO.Pipelines`).

## ⚡ Performance Benchmarks

* **Data Processed:** 1 GB (1,073,741,824 bytes)
* **Log Records:** 14,836,097 lines
* **Processing Time:** ~7.72 seconds
* **Peak Memory Usage:** **~6.17 MB** (Bounded execution, zero OOM risk)
* **Throughput:** ~131+ MB/s across 8 worker threads

## 🛠️ Key Features & Architecture

* **Producer-Consumer Pattern:** Implemented via `System.Threading.Channels` with backpressure (`BoundedChannelFullMode.Wait`).
* **Streaming Input:** Efficient buffer streaming using `System.IO.Pipelines`.
* **Zero-OOM Guarantee:** Constant ~6 MB memory footprint regardless of log file size (1 GB to 100 GB+).
* **AI Spec-Driven Development:** Designed using **BMAD-METHOD** (PRD & Architecture) and implemented via **OpenSpec**.

## 🚀 How to Run

```bash
# Build project
dotnet build

# Generate 1GB synthetic sample log
dotnet run -- generate 1GB sample.log

# Run parallel log processing engine with 8 workers
dotnet run -- run sample.log --workers 8

# Run benchmark harness
dotnet run -- benchmark sample.log
```
