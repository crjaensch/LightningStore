namespace LightningStore.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using Shouldly;
    using Xunit;

    public class ChangeStreamTests : IDisposable
    {
        private readonly ChangeStream _changeStream;
        private readonly string _path;

        public ChangeStreamTests()
        {
            _path = Path.Combine(Path.GetTempPath(), "LightningStore", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_path);
            
            _changeStream = new ChangeStream(_path);
        }

        [Fact]
        public void ByteOrderMatters()
        {
            const int count = 258;
            for (int i = 0; i < count; i++)
            {
                _changeStream.Append(BitConverter.GetBytes(i));
            }
            var cp = _changeStream.GetLastCheckpoint();
            BitConverter.ToInt32(cp.Value, 0).ShouldBe(count-1);
            cp.Key.ShouldBe(count-1);
        }

        [Theory]
        [InlineData(5, 2, 6)]
        [InlineData(-1, 2, 0)]
        [InlineData(-1000, 2, 0)]
        [InlineData(8, 1, 9)]
        [InlineData(0, 2, 1)]
        public void CanReadFromCheckpoint(long checkpoint, int expectedCount, int firstRead)
        {
            _changeStream.Append(Enumerable.Range(0, 10).Select(BitConverter.GetBytes).ToArray());
            var read = _changeStream.ReadAfter(checkpoint, 2).ToArray();

            read.Length.ShouldBe(expectedCount);

            for (int i = 0; i < expectedCount; i++, firstRead++)
            {
                read[i].Key.ShouldBe(firstRead);
                BitConverter.ToInt32(read[i].Value, 0).ShouldBe(firstRead);
            }
        }

        [Theory]
        [InlineData(5, 2, 4)]
        [InlineData(100, 2, 9)]
        [InlineData(-1, 2, 9)]
        [InlineData(-1000, 2, 9)]
        [InlineData(1, 1, 0)]
        [InlineData(0, 0, 9)]
        public void CanReadBackwords(long checkpoint, int expectedCount, int firstRead)
        {
            _changeStream.Append(Enumerable.Range(0, 10).Select(BitConverter.GetBytes).ToArray());

            var read = _changeStream.ReadBackwords(checkpoint, 2).ToArray();

            read.Length.ShouldBe(expectedCount);

            for (int i = 0; i < expectedCount; i++, firstRead--)
            {
                read[i].Key.ShouldBe(firstRead);
                BitConverter.ToInt32(read[i].Value, 0).ShouldBe(firstRead);
            }
        }

        public void Dispose()
        {
            _changeStream.Dispose();
            try
            {
                Directory.Delete(_path, true);
            }
            catch { }
        }
    }
}
