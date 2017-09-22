namespace LightningStore
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Text;

    public static class Serializer
    {
        private static Jil.Options s_options = new Jil.Options(
            false, true, false, Jil.DateTimeFormat.MillisecondsSinceUnixEpoch,
            true, Jil.UnspecifiedDateTimeKindBehavior.IsUTC
        );

        public static T DeserializeJson<T>(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var input = new StreamReader(ms, Encoding.UTF8))
            {
                return Jil.JSON.Deserialize<T>(input, s_options);
            }
        }

        public static byte[] SerializeJson<T>(T obj)
        {
            using (var ms = new MemoryStream())
            {
                using (var output = new StreamWriter(ms, Encoding.UTF8))
                {
                    Jil.JSON.Serialize(obj, output, s_options);
                }
                return ms.ToArray();
            }
        }

        public static T DeserializeGzipJson<T>(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var gzip = new DeflateStream(ms, CompressionMode.Decompress, true))
            using (var input = new StreamReader(gzip, Encoding.UTF8))
            {
                return Jil.JSON.Deserialize<T>(input, s_options);
            }
        }

        public static byte[] SerializeGzipJson<T>(T obj)
        {
            using (var ms = new MemoryStream())
            {
                using (var gzip = new DeflateStream(ms, CompressionLevel.Optimal, true))
                using (var output = new StreamWriter(gzip, Encoding.UTF8))
                {
                    Jil.JSON.Serialize(obj, output, s_options);
                }
                return ms.ToArray();
            }
        }

        public static readonly Func<string, byte[]> SerializeString = Encoding.UTF8.GetBytes;
        public static readonly Func<byte[], string> DeserializeString = Encoding.UTF8.GetString;
        public static byte[] SerializeGuid(Guid g) => g.ToByteArray();
        public static Guid DeserializeGuid(byte[] bs) => new Guid(bs);
        public static readonly Func<long, byte[]> SerializeLong = BitConverter.GetBytes;
        public static long DeserializeLong(byte[] bs) => BitConverter.ToInt64(bs, 0);
    }
}