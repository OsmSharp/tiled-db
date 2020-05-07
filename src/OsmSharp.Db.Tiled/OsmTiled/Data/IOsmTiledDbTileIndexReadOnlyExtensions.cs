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
        
        public static IEnumerable<long> LowestPointersFor(this IOsmTiledDbTileIndexReadOnly tileIndex, IEnumerable<uint> tiles)
        {
            foreach (var tile in tiles)
            {
                var pointer = tileIndex.Get(tile);
                if (pointer == OsmTiledDbTileIndex.EmptyTile) continue;
                if (pointer == tileIndex.Default) continue;

                yield return pointer;
            }
        }
    }
}