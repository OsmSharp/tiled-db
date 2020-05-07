using System.IO;
using OsmSharp.Db.Tiled.Logging;

namespace OsmSharp.Db.Tiled.IO
{
    internal static class StreamExtensions
    {
        public static Stream ToMemoryStreamSmall(this Stream stream, long maxLength)
        {
            if (stream.Length >= maxLength) return stream;
            
            //Log.Default.Debug($"Loading stream in memory, stream has {stream.Length} bytes, with a {maxLength} max set.");
            var memoryStream = new MemoryStream();
            stream.CopyTo(memoryStream);
            memoryStream.Seek(0, SeekOrigin.Begin);
            stream.Dispose();
            return memoryStream;
        }
    }
}