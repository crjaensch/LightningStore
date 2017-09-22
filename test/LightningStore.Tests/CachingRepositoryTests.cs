namespace LightningStore.Tests
{
    using System;
    using System.IO;
    using Shouldly;
    using Xunit;

    public class CachingRepositoryTests : IDisposable
    {
        private readonly ObjectRepository<string, Document> _repo;
        private readonly string _path;
        
        public CachingRepositoryTests()
        {
            _path = Path.Combine(Path.GetTempPath(), "LightningStore", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_path);
            
            _repo = new ObjectRepository<string, Document>(
                new DefaultObjectRepositorySettings<Document>(_path));
        }

        [Fact]
        public void Does_not_store_anything_if_not_committed()
        {
            bool triggered = false;
            var key = Guid.NewGuid().ToString("N");

            using (var session = new CachingRepository<string, Document>(_repo,
                _ => triggered = true,
                _ => triggered = true))
            {
                session.Run(key, d => { d.Name = "test"; d.Value = "test123"; });
                session[key].Name.ShouldBe("test");
                session[key].Value.ShouldBe("test123");
            }

            triggered.ShouldBeFalse();
            _repo.Get(key).ShouldBeNull();
        }

        [Fact]
        public void Stores_and_deletes()
        {
            bool deleted = false;
            bool inserted = false;
            var key = Guid.NewGuid().ToString("N");

            using (var session = new CachingRepository<string, Document>(_repo,
                _ => deleted = true,
                _ => inserted = true))
            {
                session.Run(key, d => { d.Name = "test"; d.Value = "test123"; });
                session.Commit();
            }

            deleted.ShouldBeFalse();
            inserted.ShouldBeTrue();
            _repo.Get(key).ShouldNotBeNull();

            inserted = false;

            using (var session = new CachingRepository<string, Document>(_repo,
                _ => deleted = true,
                _ => inserted = true))
            {
                session.Delete(key);
                session.IsDeleted(key).ShouldBeTrue();
                session.Commit();
            }

            deleted.ShouldBeTrue();
            inserted.ShouldBeFalse();
            _repo.Get(key).ShouldBeNull();
        }

        [Fact]
        public void Cannot_commitsession_twice()
        {
            var key = Guid.NewGuid().ToString("N");

            using (var session = new CachingRepository<string, Document>(_repo))
            {
                session.Run(key, d => { d.Name = "test"; d.Value = "test123"; });
                session.Commit();
                Should.Throw<ObjectDisposedException>(() => session.Commit());
            }
        }

        public void Dispose()
        {
            _repo.Dispose();
            try
            {
                Directory.Delete(_path, true);
            }
            catch { }
        }

        private class Document
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}