using System.Collections.Generic;
using System.IO;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.Tiles
{
    internal static class BinaryStreamHelper
    {
        public static Stream Create(IEnumerable<OsmGeo> osmGeos)
        {
            var stream = new MemoryStream();
            foreach (var osmGeo in osmGeos)
            {
                stream.Append(osmGeo);
            }

            stream.Seek(0, SeekOrigin.Begin);
            return stream;
        }
    }
}