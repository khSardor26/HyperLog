using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace HyperLog.Tools
{
    public static class SyntheticLogGenerator
    {
        private static readonly string[] Methods = new[] { "GET", "POST", "PUT", "DELETE" };
        private static readonly string[] Paths = new[] { "/api/v1/resource", "/api/v1/items", "/health", "/login", "/metrics" };
        private static readonly string[] Statuses = new[] { "200", "201", "400", "401", "403", "404", "500" };

        public static async Task GenerateAsync(string path, long targetBytes, int bufferKb = 64)
        {
            var rand = new Random(42);
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
            }

            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: bufferKb * 1024, useAsync: true);
            using var sw = new StreamWriter(fs, Encoding.UTF8);

            long written = 0;
            var timestamp = DateTime.UtcNow;

            while (written < targetBytes)
            {
                var ip = $"192.168.{rand.Next(0, 255)}.{rand.Next(1, 254)}";
                var method = Methods[rand.Next(Methods.Length)];
                var p = Paths[rand.Next(Paths.Length)];
                var status = Statuses[rand.Next(Statuses.Length)];
                var latency = rand.Next(1, 2000);
                var line = $"{timestamp:yyyy-MM-ddTHH:mm:ssZ} [{(rand.NextDouble() < 0.05 ? "ERROR" : "INFO")}] {ip} {method} {p} - {status} - {latency}ms";

                await sw.WriteLineAsync(line);

                // approximate bytes (UTF8)
                written += Encoding.UTF8.GetByteCount(line) + 1;
                timestamp = timestamp.AddMilliseconds(rand.Next(0, 100));

                if (written % (10 * 1024 * 1024) < 200) // flush periodically
                    await sw.FlushAsync();
            }

            await sw.FlushAsync();
        }
    }
}
