namespace LightningStore
{
    using System;

    public class ObjectRepositorySettings<TKey, T>
    {
        public readonly string Path;
        public readonly Func<T, byte[]> Serialize;
        public readonly Func<byte[], T> Deserialize;
        public readonly Func<TKey, byte[]> SerializeKey;
        public readonly Func<byte[], TKey> DeserializeKey;

        public ObjectRepositorySettings(string path,
            Func<T, byte[]> serialize,
            Func<byte[], T> deserialize,
            Func<TKey, byte[]> serializeKey,
            Func<byte[], TKey> deserializeKey)
        {
            Path = path;
            Serialize = serialize;
            Deserialize = deserialize;
            SerializeKey = serializeKey;
            DeserializeKey = deserializeKey;
        }
    }
}