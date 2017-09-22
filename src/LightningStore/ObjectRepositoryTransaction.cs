namespace LightningStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using LightningDB;

    public class ObjectRepositoryTransaction<TKey, T> : IDisposable
    {
        private readonly ObjectRepositorySettings<TKey, T> _settings;
        private readonly LightningTransaction _tx;
        private readonly LightningDatabase _db;

        internal ObjectRepositoryTransaction(
            ObjectRepositorySettings<TKey, T> settings,
            LightningTransaction tx,
            LightningDatabase db)
        {
            _settings = settings;
            _tx = tx;
            _db = db;
        }

        public T Get(TKey key)
        {
            byte[] obj;
            if (_tx.TryGet(_db, _settings.SerializeKey(key), out obj))
                return _settings.Deserialize(obj);
            else return default(T);
        }

        public IEnumerable<T> Get(params TKey[] keys)
        {
            byte[] obj;
            foreach (var key in keys)
            {
                if (_tx.TryGet(_db, _settings.SerializeKey(key), out obj))
                    yield return _settings.Deserialize(obj);
                else yield return default(T);
            }
        }

        public long Count => _tx.GetEntriesCount(_db);

        public void Put(TKey key, T data) =>
            _tx.Put(_db, _settings.SerializeKey(key), _settings.Serialize(data), PutOptions.NoDuplicateData);

        public void Put(IEnumerable<KeyValuePair<TKey, T>> data)
        {
            foreach (var p in data)
            {
                Put(p.Key, p.Value);
            }
        }

        public IEnumerable<KeyValuePair<TKey, T>> List()
        {
            using (var c = _tx.CreateCursor(_db))
                while (c.MoveNext())
                    yield return new KeyValuePair<TKey, T>(
                        _settings.DeserializeKey(c.Current.Key),
                        _settings.Deserialize(c.Current.Value));
        }

        public void Delete(params TKey[] keys) => DeleteImpl(keys);
        public void Delete(IEnumerable<TKey> keys) => DeleteImpl(keys);
        private void DeleteImpl(IEnumerable<TKey> keys)
        {
            foreach (var key in keys.Select(_settings.SerializeKey))
            {
                if (_tx.ContainsKey(_db, key)) _tx.Delete(_db, key);
            }
        }

        public void Commit() => _tx.Commit();

        public void Dispose() => _tx.Dispose();
    }
}