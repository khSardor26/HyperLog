using System.Threading.Channels;

namespace HyperLog.Core
{
    // Simple wrapper exposing a bounded channel for byte chunks.
    public class BoundedLogQueue
    {
        private readonly Channel<ReadOnlyMemory<byte>> _channel;

        public BoundedLogQueue(int capacity)
        {
            var options = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            };
            _channel = Channel.CreateBounded<ReadOnlyMemory<byte>>(options);
        }

        public ChannelWriter<ReadOnlyMemory<byte>> Writer => _channel.Writer;
        public ChannelReader<ReadOnlyMemory<byte>> Reader => _channel.Reader;
    }
}
