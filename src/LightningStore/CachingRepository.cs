namespace LightningStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using LightningDB;

    public class CachingRepository<TKey, T> : IDisposable
        where T: new()
    {
        private bool _disposed = false;
        private readonly ObjectRepository<TKey, T> _repo;
        private readonly Lazy<ObjectRepositoryTransaction<TKey, T>> _tx;
        private readonly ConcurrentDictionary<TKey, T> _cache = new ConcurrentDictionary<TKey, T>();
        private readonly HashSet<TKey> _deletes = new HashSet<TKey>();

        private readonly Action<IReadOnlyCollection<TKey>> _onCommittedDeletes;
        private readonly Action<IReadOnlyDictionary<TKey, T>> _onCommittedUpserts;

        public CachingRepository(
            ObjectRepository<TKey, T> repo,
            Action<IReadOnlyCollection<TKey>> onCommittedDeletes = null,
            Action<IReadOnlyDictionary<TKey, T>> onCommittedUpserts = null)
        {
            _repo = repo;
            _tx = new Lazy<ObjectRepositoryTransaction<TKey, T>>(() => _repo.BeginTransaction());
            _onCommittedDeletes = onCommittedDeletes ?? (_ => {});
            _onCommittedUpserts = onCommittedUpserts ?? (_ => {});
        }

        public void Run(TKey key, Action<T> action)
        {
            if (action != null)
            {
                action(this[key]);
            }
        }

        public void Commit()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CachingRepository<TKey, T>));

            if (!_tx.IsValueCreated && !_deletes.Any() && !_cache.Any())
                return;

            _disposed = true;
            try
            {
                try
                {
                    if (_deletes.Any()) _tx.Value.Delete(_deletes);
                    if (_cache.Any()) _tx.Value.Put(_cache);
                    _tx.Value.Commit();
                    if (_deletes.Any()) _onCommittedDeletes(_deletes);
                    if (_cache.Any()) _onCommittedUpserts(_cache);
                }
                finally
                {
                    _tx.Value.Dispose();
                }
            }
            catch (LightningException ex) when (ex.StatusCode == LightningDB.Native.Lmdb.MDB_MAP_FULL)
            {
                if (_deletes.Any()) _repo.Delete(_deletes.ToArray());
                if (_cache.Any()) _repo.Put(_cache);
            }
        }

        public bool IsDeleted(TKey key) => _deletes.Contains(key);

        public T this[TKey key]
        {
            get
            {
                return _cache.GetOrAdd(key, _ =>
                {
                    var t = _tx.Value.Get(key);
                    return (t == null) ? new T() : t;
                });
            }
            set
            {
                _cache[key] = value;
            }
        }

        public void Dispose()
        {
            if (!_disposed && _tx.IsValueCreated)
            {
                _disposed = true;
                _tx.Value.Dispose();
            }
        }

        public void Delete(TKey key)
        {
            T _;
            _cache.TryRemove(key, out _);
            _deletes.Add(key);
        }
    }
}