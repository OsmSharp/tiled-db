using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal static class IOsmTiledDbTileIndexReadOnlyExtensions
    {
        public static bool HasTile(this IOsmTiledDbTileIndexReadOnly tileIndex, uint tile)
        {
            var pointer = tileIndex.Get(tile);

            return pointer != tileIndex.Default;
        }
        
        public static long? LowestPointerFor(this IOsmTiledDbTileIndexReadOnly tileIndex, IEnumerable<uint> tiles)
        {
            long? lowest = null;

            foreach (var tile in tiles)
            {
                var pointer = tileIndex.Get(tile);
                if (pointer == OsmTiledDbTileIndex.EmptyTile) continue;
                if (pointer == tileIndex.Default) continue;
                if (lowest == null)
                {
                    lowest = pointer;
                    continue;
                }

                if (lowest.Value > pointer)
                {
                    lowest = pointer;
                    continue;
                }
            }

            return lowest;
        }
    }
}