namespace LightningStore
{
    using RocksDbSharp;
    using System;
    using System.Collections.Generic;

    public class ObjectRepository<T, TKey> : IDisposable
    {
        private readonly ObjectRepositorySettings<T, TKey> _settings;
        private static readonly DbOptions s_defaultDbConfig = new DbOptions().SetCreateIfMissing(true);

        public ObjectRepository(ObjectRepositorySettings<T, TKey> settings)
        {
            _settings = settings;
            EnsureCreated();
        }
        private void EnsureCreated()
        {
            using (var db = RocksDb.Open(s_defaultDbConfig, _settings.Path, new ColumnFamilies()))
                db.GetProperty("rocksdb.estimate-num-keys");
        }

        public ObjectRepositoryTransaction<T, TKey> BeginTransaction(bool readOnly = false)
        {
            var db = readOnly
                ? RocksDb.OpenReadOnly(s_defaultDbConfig, _settings.Path, new ColumnFamilies(), false)
                : RocksDb.Open(s_defaultDbConfig, _settings.Path, new ColumnFamilies());
            return new ObjectRepositoryTransaction<T, TKey>(_settings, db);
        }

        public T Get(TKey key)
        {
            using (var tx = BeginTransaction(true))
                return tx.Get(key);
        }

        public IEnumerable<T> Get(params TKey[] keys)
        {
            using (var tx = BeginTransaction(true))
            {
                foreach (var key in keys)
                {
                    yield return tx.Get(key);
                }
            }
        }

        public long Count
        {
            get
            {
                using (var tx = BeginTransaction(true))
                {
                    return tx.Count;
                }
            }
        }

        public void Put(TKey key, T data)
        {
            using (var db = RocksDb.Open(s_defaultDbConfig, _settings.Path, new ColumnFamilies()))
            {
                db.Put(_settings.SerializeKey(key), _settings.Serialize(data));
            }
        }

        public void Put(IEnumerable<KeyValuePair<TKey, T>> data)
        {
            using (var tx = BeginTransaction())
            {
                tx.Put(data);
                tx.Commit();
            }
        }

        public IEnumerable<KeyValuePair<TKey, T>> List()
        {
            using (var tx = BeginTransaction(true))
                foreach (var p in tx.List())
                    yield return p;
        }

        public void Dispose() {}
    }
}