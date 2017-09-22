namespace LightningStore.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Shouldly;
    using Xunit;

    public class ObjectRepositoryTests : IDisposable
    {
        private readonly ObjectRepository<string, Document> _repo;
        private readonly string _path;

        public ObjectRepositoryTests()
        {
            _path = Path.Combine(Path.GetTempPath(), "LightningStore", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_path);
            
            _repo = new ObjectRepository<string, Document>(
                new DefaultObjectRepositorySettings<Document>(_path));
        }

        [Fact]
        public void CanStoreAndRetreive()
        {
            var doc = new Document { Name = "test", Value = "some value" };
            _repo.Put(doc.Name, doc);
            var restored = _repo.Get(doc.Name);
            restored.ShouldNotBeNull();
            restored.Name.ShouldBe(doc.Name);
            restored.Value.ShouldBe(doc.Value);
        }

        [Fact]
        public void WhenDoesNotExistReturnsNull()
        {
            var restored = _repo.Get("does not exist");
            restored.ShouldBeNull();
        }

        [Fact]
        public void WheDeletedDoesNotReturnAnything()
        {
            var doc = new Document { Name = "test delete", Value = "some value" };
            _repo.Put(doc.Name, doc);
            _repo.Delete(doc.Name);

            var retreived = _repo.Get(doc.Name);
            retreived.ShouldBeNull();
        }

        [Fact]
        public void WheDeletingNonExistentShouldNotThrow()
        {
            _repo.Delete("does not exist, but deleting");
        }

        [Fact]
        public void CanListAllStoredDocs()
        {
            var docs = Enumerable.Range(0, 10)
                .Select(x => new Document { Name = $"{x:00}", Value = $"The value of {x:00}" })
                .ToArray();

            _repo.Put(docs.Select(d => new KeyValuePair<string, Document>(d.Name, d)));

            var all = _repo.List().OrderBy(x => x.Key).ToArray();

            all.Length.ShouldBe(docs.Length);
            foreach (var t in all.Zip(docs, (a, b) => Tuple.Create(a, b)))
            {
                t.Item1.Key.ShouldBe(t.Item2.Name);
                t.Item1.Value.Name.ShouldBe(t.Item2.Name);
                t.Item1.Value.Value.ShouldBe(t.Item2.Value);
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

        public class Document
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}