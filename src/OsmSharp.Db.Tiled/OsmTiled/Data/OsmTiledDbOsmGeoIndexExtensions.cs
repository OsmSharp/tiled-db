using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal static class OsmTiledDbOsmGeoIndexExtensions
    {
        public static IEnumerable<(long pointer, OsmGeoKey key)> GetAll(this OsmTiledDbOsmGeoIndex osmGeoIndex, IEnumerable<OsmGeoKey> keys)
        {
            foreach (var key in keys)
            {
                var pointer = osmGeoIndex.Get(key);
                if (pointer == null) continue;

                yield return (pointer.Value, key);
            }
        }
    }
}