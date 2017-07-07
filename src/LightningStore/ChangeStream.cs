namespace LightningStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using LightningDB;

    public class ChangeStream : IDisposable
    {
        private static readonly DatabaseConfiguration s_defaultDbConfig = new DatabaseConfiguration
        {
            Flags = (BitConverter.IsLittleEndian) ? DatabaseOpenFlags.ReverseKey : DatabaseOpenFlags.None
        };

        private readonly LightningEnvironment _env;

        public ChangeStream(string dirPath)
        {
            _env = new LightningEnvironment(dirPath);
            _env.Open();
        }

        private KeyValuePair<long,byte[]> Convert(KeyValuePair<byte[],byte[]> pair) =>
            new KeyValuePair<long, byte[]>(BitConverter.ToInt64(pair.Key, 0), pair.Value);

        public KeyValuePair<long,byte[]> GetLastCheckpoint()
        {
            using (var tx = _env.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase(configuration: s_defaultDbConfig))
            using (var c = tx.CreateCursor(db))
            {
                if (c.MoveToLast()) return Convert(c.Current);
                else return new KeyValuePair<long, byte[]>(-1L, null);
            }
        }

        public long Append(params byte[][] values)
        {
            return _env.WithAutogrowth(() =>
            {
                using (var tx = _env.BeginTransaction())
                using (var db = tx.OpenDatabase(configuration: s_defaultDbConfig))
                {
                    var nextKey = 0L;
                    using (var c = tx.CreateCursor(db))
                    {
                        if (c.MoveToLast())
                            nextKey = BitConverter.ToInt64(c.Current.Key, 0) + 1L;
                    }
                    foreach (var value in values)
                    {
                        tx.Put(db, BitConverter.GetBytes(nextKey), value, PutOptions.AppendData | PutOptions.NoOverwrite);
                        nextKey++;
                    }
                    tx.Commit();
                    return nextKey-1L;
                }
            });
        }

        public IEnumerable<KeyValuePair<long, byte[]>> ReadAfter(long key, int maxCount = 512)
        {
            var byteKey = BitConverter.GetBytes(key);
            using (var tx = _env.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase(configuration: s_defaultDbConfig))
            using (var c = tx.CreateCursor(db))
            {
                if (key >= 0)
                { 
                    if (!c.MoveToFirstAfter(byteKey)) yield break;
                }
                var count = 0;
                while (count < maxCount && c.MoveNext())
                {
                    yield return Convert(c.Current);
                    count++;
                }
            }
        }
        public IEnumerable<KeyValuePair<long, byte[]>> ReadBackwords(long key, int maxCount = 512)
        {
            var byteKey = BitConverter.GetBytes(key);
            using (var tx = _env.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var db = tx.OpenDatabase(configuration: s_defaultDbConfig))
            using (var c = tx.CreateCursor(db))
            {
                c.MoveTo(byteKey);
                var count = 0;
                while (count < maxCount && c.MovePrev())
                {
                    yield return Convert(c.Current);
                    count++;
                }
            }
        }

        public void Dispose() => _env.Dispose();
    }
}
