namespace LightningStore.Tests
{
    using System;
    using System.IO;
    using Shouldly;
    using Xunit;

    public class CheckpointStoreTests : IDisposable
    {
        private readonly string _filePath;
        private readonly CheckpointStore _checkpointStore;

        public CheckpointStoreTests()
        {
            _filePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            _checkpointStore = new CheckpointStore(_filePath);
        }

        [Theory]
        [InlineData(null)]
        [InlineData(1)]
        [InlineData(-1)]
        [InlineData(long.MinValue)]
        [InlineData(long.MaxValue)]
        [InlineData(0)]
        [InlineData(100)]
        public void Can_store_and_read_checkpoint(long? checkpoint)
        {
            _checkpointStore.Write(checkpoint);
            var returned = _checkpointStore.Read();
            returned.ShouldBe(checkpoint);
        }

        [Theory]
        [InlineData(null)]
        [InlineData(-1)]
        [InlineData(1)]
        [InlineData(long.MinValue)]
        [InlineData(long.MaxValue)]
        [InlineData(0)]
        [InlineData(100)]
        public void Can_store_and_read_by_readonly(long? checkpoint)
        {
            _checkpointStore.Write(checkpoint);
            using (var readonlyStore = new CheckpointStore(_filePath, true))
            {
                var returned = readonlyStore.Read();
                returned.ShouldBe(checkpoint);
            }
        }

        public void Dispose()
        {
            _checkpointStore.Dispose();
            File.Delete(_filePath);
        }
    }
}
