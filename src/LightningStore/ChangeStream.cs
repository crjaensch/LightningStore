namespace LightningStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using RocksDbSharp;

    using static Serializer;

    public class ChangeStream : IDisposable
    {
        private static readonly DbOptions s_defaultDbConfig = new DbOptions().SetCreateIfMissing(true);

        private readonly string _path;

        public ChangeStream(string dirPath)
        {
            _path = dirPath;
            EnsureCreated();
        }

        private void EnsureCreated()
        {
            using (var db = RocksDb.Open(s_defaultDbConfig, _path, new ColumnFamilies()))
                db.GetProperty("rocksdb.estimate-num-keys");
        }

        private KeyValuePair<long,byte[]> Convert(KeyValuePair<byte[],byte[]> pair) =>
            new KeyValuePair<long, byte[]>(DeserializeLong(pair.Key), pair.Value);

        public KeyValuePair<long,byte[]> GetLastCheckpoint()
        {
            using (var db = RocksDb.OpenReadOnly(s_defaultDbConfig, _path, new ColumnFamilies(), false))
            {
                using (var iter = db.NewIterator())
                {
                    iter.SeekToLast();
                    if (!iter.Valid()) return new KeyValuePair<long, byte[]>(-1L, null);
                    else return Convert(new KeyValuePair<byte[], byte[]>(iter.Key(), iter.Value()));
                }
            }
        }

        public long Append(params byte[][] values)
        {
            using (var db = RocksDb.Open(s_defaultDbConfig, _path, new ColumnFamilies()))
            {
                var nextKey = 0L;
                using (var iter = db.NewIterator())
                {
                    iter.SeekToLast();
                    if (iter.Valid()) nextKey = DeserializeLong(iter.Key()) + 1;
                }
                using (var batch = new WriteBatch())
                {
                    foreach (var value in values)
                    {
                        batch.Put(SerializeLong(nextKey), value);
                        nextKey++;
                    }
                    db.Write(batch);
                }
                return nextKey - 1L;
            }
        }

        public IEnumerable<KeyValuePair<long, byte[]>> ReadAfter(long key, int maxCount = 512)
        {
            var byteKey = SerializeLong(key);
            using (var db = RocksDb.OpenReadOnly(s_defaultDbConfig, _path, new ColumnFamilies(), false))
            using (var iter = db.NewIterator(readOptions: new ReadOptions()))
            {
                if (key >= 0)
                {
                    iter.Seek(byteKey);
                    if (!iter.Valid()) yield break;
                    iter.Next();
                }
                else iter.SeekToFirst();

                var count = 0;
                while (count < maxCount && iter.Valid())
                {
                    yield return Convert(new KeyValuePair<byte[], byte[]>(iter.Key(), iter.Value()));
                    iter.Next();
                    count++;
                }
            }
        }
        public IEnumerable<KeyValuePair<long, byte[]>> ReadBackwords(long key, int maxCount = 512)
        {
            var byteKey = SerializeLong(key);
            using (var db = RocksDb.OpenReadOnly(s_defaultDbConfig, _path, new ColumnFamilies(), false))
            using (var iter = db.NewIterator(readOptions: new ReadOptions()))
            {
                if (key >= 0)
                {
                    iter.Seek(byteKey);
                    if (iter.Valid()) iter.Prev();
                    else iter.SeekToLast();
                }
                else iter.SeekToLast();
                var count = 0;
                while (count < maxCount && iter.Valid())
                {
                    yield return Convert(new KeyValuePair<byte[], byte[]>(iter.Key(), iter.Value()));
                    iter.Prev();
                    count++;
                }
            }
        }

        public void Dispose() {}
    }
}
