namespace LightningStore.Tests
{
    using Shouldly;
    internal static class ObjectExtensions
    {
        public static void ShouldBeEqual<T>(this T actual, T expected)
        {
            
            var compare = new KellermanSoftware.CompareNetObjects.CompareLogic();
            var result = compare.Compare(actual, expected);
            result.Differences.ShouldBeEmpty();
        }
    }
}