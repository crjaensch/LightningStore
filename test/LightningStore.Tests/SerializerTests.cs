namespace LightningStore.Tests
{
    using System;
    using Shouldly;
    using Xunit;

    public class SerializerTests
    {
        private static readonly Data s_data = new Data
        {
            Name = Guid.NewGuid().ToString(),
            Values = new [] { "1", "test", "some" },
            Children = new []
            {
                new Data
                {
                    Name = Guid.NewGuid().ToString(),
                    Values = new [] { "2", "5", "x" }
                }
            }
        };

        [Fact]
        public void Can_serialize_and_deserialize_gzip_json()
        {

            var serialized = Serializer.SerializeGzipJson(s_data);
            var deserialized = Serializer.DeserializeGzipJson<Data>(serialized);
            deserialized.ShouldBeEqual(s_data);
        }

        [Fact]
        public void Can_serialize_and_deserialize_json()
        {

            var serialized = Serializer.SerializeJson(s_data);
            var deserialized = Serializer.DeserializeJson<Data>(serialized);
            deserialized.ShouldBeEqual(s_data);
        }

        [Fact]
        public void Can_serialize_and_deserialize_guid()
        {
            var sut = Guid.NewGuid();
            var serialized = Serializer.SerializeGuid(sut);
            var deserialized = Serializer.DeserializeGuid(serialized);
            deserialized.ShouldBe(sut);
        }

        private class Data
        {
            public string Name { get; set; }
            public string[] Values { get; set; }
            public Data[] Children { get; set; }
        }
    }
}