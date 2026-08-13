using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using HyperLog.Core;
using HyperLog.IO;
using HyperLog.Workers;

namespace HyperLog.Tools
{
    public static class BenchmarkHarness
    {
        public static async Task RunBaselineAsync(string path)
        {
            var sw = Stopwatch.StartNew();
            long lines = 0;
            foreach (var _ in File.ReadLines(path))
            {
                lines++;
            }
            sw.Stop();
            var bytes = new FileInfo(path).Length;
            Console.WriteLine($"Baseline: lines={lines}, bytes={bytes}, time={sw.Elapsed.TotalSeconds:F2}s, throughput={bytes / 1024.0 / 1024.0 / sw.Elapsed.TotalSeconds:F2} MB/s");
        }

        public static async Task RunPipelineAsync(string path, int workers, int chunkSize)
        {
            var fi = new FileInfo(path);
            var totalBytes = fi.Length;

            var queueCapacity = workers * 32;
            var bounded = new BoundedLogQueue(queueCapacity);
            var output = Channel.CreateBounded<LogRecord>(new BoundedChannelOptions(workers * 64) { FullMode = BoundedChannelFullMode.Wait });

            using var cts = new CancellationTokenSource();

            long processed = 0;
            var consumer = Task.Run(async () =>
            {
                await foreach (var r in output.Reader.ReadAllAsync(cts.Token))
                {
                    Interlocked.Increment(ref processed);
                }
            }, cts.Token);

            var sw = Stopwatch.StartNew();
            var readerTask = LogChunkReader.ReadFileToChannelAsync(path, bounded.Writer, chunkSize, cts.Token);
            var parserTask = LogParserWorker.StartWorkersAsync(bounded.Reader, output.Writer, workers, cts.Token);

            await Task.WhenAll(readerTask, parserTask);

            // give consumer a moment
            await Task.Delay(200);
            sw.Stop();

            Console.WriteLine($"Pipeline: processed={processed}, bytes={totalBytes}, time={sw.Elapsed.TotalSeconds:F2}s, throughput={totalBytes / 1024.0 / 1024.0 / sw.Elapsed.TotalSeconds:F2} MB/s, memoryMB={GC.GetTotalMemory(false) / 1024.0 / 1024.0:F2}");

            cts.Cancel();
            await consumer;
        }
    }
}
