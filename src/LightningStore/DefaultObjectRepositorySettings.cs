namespace LightningStore
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Text;

    public class DefaultObjectRepositorySettings<T> : ObjectRepositorySettings<T, string>
    {
        private static Jil.Options s_options = new Jil.Options(
            false, true, false, Jil.DateTimeFormat.MillisecondsSinceUnixEpoch,
            false, Jil.UnspecifiedDateTimeKindBehavior.IsUTC
        );

        public DefaultObjectRepositorySettings(string path)
            : base(path, SerializeImpl, DeserializeImpl, Encoding.UTF8.GetBytes, Encoding.UTF8.GetString)
        {
        }

        private static byte[] SerializeImpl(T obj)
        {
            using (var ms = new MemoryStream())
            {
                using (var gzip = new DeflateStream(ms, CompressionLevel.Optimal, true))
                using (var output = new StreamWriter(gzip, Encoding.UTF8))
                {
                    Jil.JSON.Serialize(obj, output, s_options);
                }
                // if (ms.Position > 4096)
                //     Console.WriteLine("EXCEEDS page size");
                return ms.ToArray();
            }
        }
        private static T DeserializeImpl(byte[] data)
        {
            if (data.Length == 0) return default(T);
            try
            {
                using (var ms = new MemoryStream(data))
                using (var gzip = new DeflateStream(ms, CompressionMode.Decompress, true))
                using (var input = new StreamReader(gzip, Encoding.UTF8))
                {
                    return Jil.JSON.Deserialize<T>(input, s_options);
                }
            }
            catch
            {
                Console.WriteLine(Encoding.UTF8.GetString(data));
                throw;
            }
        }
    }
}