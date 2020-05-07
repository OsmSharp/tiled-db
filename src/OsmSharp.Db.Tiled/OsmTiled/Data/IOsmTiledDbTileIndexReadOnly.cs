using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal interface IOsmTiledDbTileIndexReadOnly
    {
        long Default { get; }
        
        long NonDefaultCount { get; }
        
        long Get(uint tile);

        IEnumerable<uint> GetTiles();
    }
}