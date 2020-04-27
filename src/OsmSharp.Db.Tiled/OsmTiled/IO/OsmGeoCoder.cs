using System;
using System.IO;
using OsmSharp.Db.Tiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    internal static class OsmGeoCoder
    {
        private const long IdTypeMask = (long) 1 << 61;

        public static long Encode(OsmGeoKey id)
        {
            return Encode(id.Type, id.Id);
        }

        public static long Encode(OsmGeoType type, long id)
        {
            return type switch
            {
                OsmGeoType.Node => id,
                OsmGeoType.Way => (id + IdTypeMask),
                OsmGeoType.Relation => (id + (IdTypeMask * 2)),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
            };
        }

        public static void Write(this Stream stream, OsmGeoKey key)
        {
            var id = Encode(key.Type, key.Id);

            stream.WriteInt64(id);
        }
    }
}