using System;

namespace HyperLog.Core
{
    // Read-only record representing a parsed log line. Fields use ReadOnlyMemory<char>
    // for low-allocation downstream processing. Converting from bytes to chars
    // currently allocates strings; optimize by decoding into pooled char buffers later.
    public readonly struct LogRecord
    {
        public ReadOnlyMemory<char> Timestamp { get; }
        public ReadOnlyMemory<char> LogLevel { get; }
        public ReadOnlyMemory<char> Message { get; }
        public ReadOnlyMemory<char> SourceIp { get; }

        public LogRecord(ReadOnlyMemory<char> timestamp, ReadOnlyMemory<char> logLevel, ReadOnlyMemory<char> message, ReadOnlyMemory<char> sourceIp)
        {
            Timestamp = timestamp;
            LogLevel = logLevel;
            Message = message;
            SourceIp = sourceIp;
        }

        public override string ToString()
        {
            // Use the ReadOnlySpan<char> -> string ctor to avoid extra copies in callers where a string is needed
            var ts = new string(Timestamp.Span);
            var lvl = new string(LogLevel.Span);
            var msg = new string(Message.Span);
            var src = new string(SourceIp.Span);
            return $"[{ts}] {lvl} {msg} ({src})";
        }
    }
}
