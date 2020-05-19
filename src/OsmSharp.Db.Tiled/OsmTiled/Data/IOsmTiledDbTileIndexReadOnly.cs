using System.Collections.Generic;
using OsmSharp.Db.Tiled.Collections;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal interface IOsmTiledDbTileIndexReadOnly : ILRUDisposable
    {
        long Default { get; }
        
        long NonDefaultCount { get; }
        
        long Get(uint tile);

        IEnumerable<uint> GetTiles();
    }
}