using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using HyperLog.Core;
using HyperLog.IO;
using HyperLog.Workers;

class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: HyperLog <path> [workerCount] [chunkSize]");
            return 1;
        }

        var path = args[0];
        int workerCount = args.Length > 1 && int.TryParse(args[1], out var w) ? w : Math.Max(1, Environment.ProcessorCount - 1);
        int chunkSize = args.Length > 2 && int.TryParse(args[2], out var c) ? c : 64 * 1024;

        var queueCapacity = workerCount * 32;
        var bounded = new BoundedLogQueue(queueCapacity);

        var output = Channel.CreateBounded<LogRecord>(new BoundedChannelOptions(workerCount * 64)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        using var cts = new CancellationTokenSource();

        Console.WriteLine($"Starting HyperLog skeleton. file={path}, workers={workerCount}, chunk={chunkSize}, capacity={queueCapacity}");

        var readerTask = LogChunkReader.ReadFileToChannelAsync(path, bounded.Writer, chunkSize, cts.Token);
        var parserTask = LogParserWorker.StartWorkersAsync(bounded.Reader, output.Writer, workerCount, cts.Token);

        // Consumer task: count processed records and print periodic stats
        var processed = 0L;
        var consumer = Task.Run(async () =>
        {
            await foreach (var rec in output.Reader.ReadAllAsync(cts.Token))
            {
                Interlocked.Increment(ref processed);
            }
        }, cts.Token);

        // Status loop
        var status = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                Console.WriteLine($"Processed: {processed} records | Memory: {GC.GetTotalMemory(false) / 1024 / 1024} MB");
                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            }
        }, cts.Token);

        await Task.WhenAll(readerTask, parserTask);

        // allow consumer to drain
        await Task.Delay(500);
        cts.Cancel();
        await Task.WhenAll(consumer, status);

        Console.WriteLine($"Finished. Total processed: {processed}");
        return 0;
    }
}
