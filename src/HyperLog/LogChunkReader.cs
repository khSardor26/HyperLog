using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HyperLog.IO
{
    // Reads from a file stream via a PipeReader and publishes byte chunks to a channel.
    // This implementation copies chunk bytes into a new array before publishing. For
    // lower-allocation operation, consider renting from ArrayPool<byte> and returning
    // buffers after parsing.
    public static class LogChunkReader
    {
        public static async Task ReadFileToChannelAsync(string filePath, ChannelWriter<ReadOnlyMemory<byte>> writer, int chunkSize = 64 * 1024, CancellationToken ct = default)
        {
            await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
            var reader = PipeReader.Create(fs);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var result = await reader.ReadAsync(ct);
                    var buffer = result.Buffer;

                    if (buffer.Length == 0 && result.IsCompleted)
                        break;

                    while (buffer.Length > 0)
                    {
                        var sliceLength = (int)Math.Min(buffer.Length, chunkSize);
                        var slice = buffer.Slice(0, sliceLength);

                        // Allocate a managed array for the slice and copy bytes into it.
                        var arr = new byte[sliceLength];
                        slice.CopyTo(arr);

                        // Publish the chunk (the reader/worker should process and drop promptly)
                        await writer.WriteAsync(new ReadOnlyMemory<byte>(arr), ct);

                        buffer = buffer.Slice(sliceLength);
                    }

                    // Tell the PipeReader we've consumed everything we examined
                    reader.AdvanceTo(result.Buffer.End);

                    if (result.IsCompleted)
                        break;
                }
            }
            finally
            {
                await reader.CompleteAsync();
                writer.Complete();
            }
        }
    }
}
