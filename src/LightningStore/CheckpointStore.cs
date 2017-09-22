namespace LightningStore
{
    using System;
    using System.IO;
    using System.IO.MemoryMappedFiles;
    using System.Runtime.InteropServices;
    using System.Threading;

    public class CheckpointStore : IDisposable
    {
        [StructLayout(LayoutKind.Explicit)]
        private struct Checkpoint
        {
            [FieldOffset(0)]
            public byte HasValue;
            [FieldOffset(1)]
            public long Value;
        }

        private static readonly int s_size = Marshal.SizeOf(typeof(Checkpoint));

        private readonly Lazy<MemoryMappedFile> _mmfile;
        private readonly Lazy<MemoryMappedViewAccessor> _mmview;

        public CheckpointStore(string filePath, bool readOnly = false)
        {
            var fileAccess = readOnly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite;
            _mmfile = new Lazy<MemoryMappedFile>(() =>
            {
                if (!File.Exists(filePath))
                {
                    using (var f = File.Create(filePath))
                        f.Write(new byte[s_size], 0, s_size);
                }
                var fs = new FileStream(filePath,
                    readOnly ? FileMode.Open : FileMode.OpenOrCreate,
                    readOnly ? FileAccess.Read : FileAccess.ReadWrite,
                    FileShare.ReadWrite,
                    s_size);
                return MemoryMappedFile.CreateFromFile(fs, null, s_size, fileAccess, HandleInheritability.Inheritable, false);
            }, LazyThreadSafetyMode.ExecutionAndPublication);
            _mmview = new Lazy<MemoryMappedViewAccessor>(
                () => _mmfile.Value.CreateViewAccessor(0, s_size, fileAccess),
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public long? Read()
        {
            Checkpoint cp;
            _mmview.Value.Read<Checkpoint>(0, out cp);
            return cp.HasValue != 0 ? cp.Value : (long?)null;
        }

        public void Write(long? checkpoint)
        {
            var cp = new Checkpoint();
            if (checkpoint.HasValue)
            {
                cp.HasValue = 1;
                cp.Value = checkpoint.Value;
            }
            _mmview.Value.Write(0, ref cp);
        }

        public void Dispose()
        {
            if (_mmview.IsValueCreated) _mmview.Value.Dispose();
            if (_mmfile.IsValueCreated) _mmfile.Value.Dispose();
        }
    }
}