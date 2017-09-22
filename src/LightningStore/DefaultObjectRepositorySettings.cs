namespace LightningStore
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Text;

    using static Serializer;

    public class DefaultObjectRepositorySettings<T> : ObjectRepositorySettings<string, T>
    {
        public DefaultObjectRepositorySettings(string path)
            : base(path, SerializeGzipJson<T>, DeserializeGzipJson<T>,
                SerializeString, DeserializeString)
        {
        }
    }
}