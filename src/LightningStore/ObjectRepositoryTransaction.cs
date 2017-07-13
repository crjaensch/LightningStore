namespace LightningStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using RocksDbSharp;

    public class ObjectRepositoryTransaction<T, TKey> : IDisposable
    {
        private readonly ObjectRepositorySettings<T, TKey> _settings;
        private readonly RocksDb _db;
        private Lazy<WriteBatch> _batch = new Lazy<WriteBatch>(() => new WriteBatch());

        internal ObjectRepositoryTransaction(
            ObjectRepositorySettings<T, TKey> settings,
            RocksDb db)
        {
            _settings = settings;
            _db = db;
        }

        public T Get(TKey key)
        {
            byte[] obj = _db.Get(_settings.SerializeKey(key));
            if (obj != null)
                return _settings.Deserialize(obj);
            else return default(T);
        }

        public IEnumerable<T> Get(params TKey[] keys)
        {
            var result = _db.MultiGet(keys.Select(_settings.SerializeKey).ToArray())
                .ToDictionary(x => _settings.DeserializeKey(x.Key), x => _settings.Deserialize(x.Value));
            foreach (var key in keys)
            {
                if (result.ContainsKey(key))
                    yield return result[key];
                else yield return default(T);
            }
        }

        public long Count => long.Parse(_db.GetProperty("rocksdb.estimate-num-keys"));

        public void Put(TKey key, T data) =>
            _batch.Value.Put(_settings.SerializeKey(key), _settings.Serialize(data));
 
        public void Put(IEnumerable<KeyValuePair<TKey, T>> data)
        {
            foreach (var p in data)
            {
                Put(p.Key, p.Value);
            }
        }

        public IEnumerable<KeyValuePair<TKey, T>> List()
        {
            using (var c = _db.NewIterator())
            {
                c.SeekToFirst();
                while (c.Valid())
                {
                    yield return new KeyValuePair<TKey, T>(
                        _settings.DeserializeKey(c.Key()),
                        _settings.Deserialize(c.Value()));
                    c.Next();
                }
            }
        }

        public void Commit()
        {
            if (_batch.IsValueCreated)
            {
                _db.Write(_batch.Value);
                _batch = new Lazy<WriteBatch>(() => new WriteBatch());
            }

        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}