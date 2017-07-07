namespace LightningStore
{
    using System;
    using System.Threading;
    using LightningDB;

    internal static class LightningEnvironmentExtensions
    {
        public static void WithAutogrowth(this LightningEnvironment env, Action action)
            => env.WithAutogrowth<bool>(() => { action(); return true; });

        public static T WithAutogrowth<T>(this LightningEnvironment env, Func<T> action)
        {
            try
            {
                return action();
            }
            catch (LightningException ex) when (ex.StatusCode == LightningDB.Native.Lmdb.MDB_MAP_FULL)
            {
                if (Monitor.TryEnter(env))
                {
                    try
                    {
                        env.MapSize = (env.MapSize == 0 ? 1048576L : env.MapSize) << 1;
                        return env.WithAutogrowthRetry(action);
                    }
                    finally
                    {
                        Monitor.Exit(env);
                    }
                }
                else
                    lock (env)
                    {
                        return env.WithAutogrowthRetry(action);
                    }
            }
        }
        private static T WithAutogrowthRetry<T>(this LightningEnvironment env, Func<T> action)
        {
            while (true)
            {
                try
                {
                    return action();
                }
                catch (LightningException ex) when (ex.StatusCode == LightningDB.Native.Lmdb.MDB_MAP_FULL)
                {
                    env.MapSize = (env.MapSize == 0 ? 1048576L : env.MapSize) << 1;
                }
            }
        }
    }
}