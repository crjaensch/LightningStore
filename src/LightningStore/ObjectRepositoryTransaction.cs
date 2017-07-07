namespace LightningStore
{
    using System;
    using System.Collections.Generic;
    using LightningDB;

    public class ObjectRepositoryTransaction<T, TKey> : IDisposable
    {
        private readonly ObjectRepositorySettings<T, TKey> _settings;
        private readonly LightningTransaction _tx;
        private readonly LightningDatabase _db;

        internal ObjectRepositoryTransaction(
            ObjectRepositorySettings<T, TKey> settings,
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

        public void Commit() => _tx.Commit();

        public void Dispose()
        {
            _db.Dispose();
            _tx.Dispose();
        }
    }
}