using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using HyperLog.Core;

namespace HyperLog.Workers
{
    // Parses byte chunks (UTF-8) into LogRecord instances. This is a simple, conservative
    // implementation that decodes bytes into strings and then exposes ReadOnlyMemory<char>.
    // Optimizations (decoding into pooled char buffers, zero-allocation field slicing)
    // can be introduced later.
    public static class LogParserWorker
    {
        public static Task StartWorkersAsync(ChannelReader<ReadOnlyMemory<byte>> reader, ChannelWriter<LogRecord> outputWriter, int workerCount, CancellationToken ct = default)
        {
            var tasks = new Task[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                tasks[i] = Task.Run(() => WorkerLoopAsync(reader, outputWriter, ct), ct);
            }
            return Task.WhenAll(tasks);
        }

        private static async Task WorkerLoopAsync(ChannelReader<ReadOnlyMemory<byte>> reader, ChannelWriter<LogRecord> outputWriter, CancellationToken ct)
        {
            await foreach (var chunk in reader.ReadAllAsync(ct))
            {
                // Decode the chunk to a string and split on newlines.
                // For production: parse using Span/SequenceReader to avoid intermediate strings.
                string text = Encoding.UTF8.GetString(chunk.Span);
                var lines = text.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);

                foreach (var line in lines)
                {
                    // Very small heuristic parse: [timestamp] LEVEL message (ip)
                    // Timestamp and Level extraction are best-effort in this skeleton.
                    var ts = string.Empty;
                    var lvl = string.Empty;
                    var msg = line;
                    var src = string.Empty;

                    // Attempt to extract space-separated first two tokens as timestamp and level
                    var firstSpace = line.IndexOf(' ');
                    if (firstSpace > 0)
                    {
                        ts = line.Substring(0, firstSpace).Trim();
                        var rest = line.Substring(firstSpace + 1).TrimStart();
                        var secondSpace = rest.IndexOf(' ');
                        if (secondSpace > 0)
                        {
                            lvl = rest.Substring(0, secondSpace).Trim();
                            msg = rest.Substring(secondSpace + 1);
                        }
                        else
                        {
                            lvl = rest;
                            msg = string.Empty;
                        }
                    }

                    var record = new LogRecord(ts.AsMemory(), lvl.AsMemory(), msg.AsMemory(), src.AsMemory());
                    await outputWriter.WriteAsync(record, ct);
                }
            }

            outputWriter.Complete();
        }
    }
}
