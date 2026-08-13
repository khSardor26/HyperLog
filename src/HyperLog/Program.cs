using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using HyperLog.Core;
using HyperLog.IO;
using HyperLog.Tools;
using HyperLog.Workers;

class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        var cmd = args[0].ToLowerInvariant();

        try
        {
            if (cmd == "generate")
            {
                if (args.Length < 3)
                {
                    Console.WriteLine("Usage: dotnet run -- generate <size> <path>");
                    return 1;
                }

                var sizeArg = args[1];
                var outPath = args[2];
                var bytes = ParseSize(sizeArg);
                Console.WriteLine($"Generating {sizeArg} ({bytes} bytes) into {outPath}...");
                await SyntheticLogGenerator.GenerateAsync(outPath, bytes);
                Console.WriteLine("Generation complete.");
                return 0;
            }
            else if (cmd == "run")
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("Usage: dotnet run -- run <path> [--workers N] [--chunkSize K]");
                    return 1;
                }

                var path = args[1];
                int workers = Math.Max(1, Environment.ProcessorCount - 1);
                int chunkSize = 64 * 1024;

                for (int i = 2; i < args.Length; i++)
                {
                    if (args[i] == "--workers" && i + 1 < args.Length && int.TryParse(args[i + 1], out var w)) { workers = w; i++; }
                    else if (args[i] == "--chunkSize" && i + 1 < args.Length && int.TryParse(args[i + 1], out var c)) { chunkSize = c; i++; }
                }

                await RunPipelineOnce(path, workers, chunkSize);
                return 0;
            }
            else if (cmd == "benchmark")
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("Usage: dotnet run -- benchmark <path> [--workers N] [--chunkSize K]");
                    return 1;
                }

                var path = args[1];
                int workers = Math.Max(1, Environment.ProcessorCount - 1);
                int chunkSize = 64 * 1024;

                for (int i = 2; i < args.Length; i++)
                {
                    if (args[i] == "--workers" && i + 1 < args.Length && int.TryParse(args[i + 1], out var w)) { workers = w; i++; }
                    else if (args[i] == "--chunkSize" && i + 1 < args.Length && int.TryParse(args[i + 1], out var c)) { chunkSize = c; i++; }
                }

                Console.WriteLine("Running baseline (File.ReadLines)...");
                await BenchmarkHarness.RunBaselineAsync(path);
                Console.WriteLine("Running pipeline benchmark...");
                await BenchmarkHarness.RunPipelineAsync(path, workers, chunkSize);
                return 0;
            }
            else
            {
                PrintUsage();
                return 1;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}\n{ex.StackTrace}");
            return 2;
        }
    }

    static void PrintUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -- generate <size> <path>       # size examples: 100MB, 1GB");
        Console.WriteLine("  dotnet run -- run <path> [--workers N] [--chunkSize K]");
        Console.WriteLine("  dotnet run -- benchmark <path> [--workers N] [--chunkSize K]");
    }

    static long ParseSize(string s)
    {
        s = s.Trim().ToUpperInvariant();
        if (s.EndsWith("GB"))
            return (long)(double.Parse(s.Substring(0, s.Length - 2)) * 1024 * 1024 * 1024);
        if (s.EndsWith("MB"))
            return (long)(double.Parse(s.Substring(0, s.Length - 2)) * 1024 * 1024);
        if (s.EndsWith("KB"))
            return (long)(double.Parse(s.Substring(0, s.Length - 2)) * 1024);
        return long.Parse(s);
    }

    static async Task RunPipelineOnce(string path, int workers, int chunkSize)
    {
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

        Console.WriteLine($"Starting run: path={path}, workers={workers}, chunkSize={chunkSize}, capacity={queueCapacity}");
        var sw = System.Diagnostics.Stopwatch.StartNew();

        var readerTask = LogChunkReader.ReadFileToChannelAsync(path, bounded.Writer, chunkSize, cts.Token);
        var parserTask = LogParserWorker.StartWorkersAsync(bounded.Reader, output.Writer, workers, cts.Token);

        await Task.WhenAll(readerTask, parserTask);

        // All parser workers have finished; complete the shared output writer so consumers can drain.
        output.Writer.TryComplete();

        // allow consumer to drain
        await Task.Delay(200);
        sw.Stop();

        Console.WriteLine($"Finished. processed={processed}, time={sw.Elapsed.TotalSeconds:F2}s, memoryMB={GC.GetTotalMemory(false)/1024.0/1024.0:F2}");

        // Wait for consumer to finish reading the completed output channel
        await consumer;
    }
}
