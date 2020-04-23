using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class OsmTiledDb : OsmTiledDbBase
    {
        private readonly OsmTiledIndex _index;
        private readonly OsmTiledLinkedStream _data;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        public OsmTiledDb(string path)
            : base(path)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _index = OsmTiledDbOperations.LoadIndex(this.Path);
        }

        internal OsmTiledDb(string path, OsmTiledDbMeta meta)
            : base(path, meta)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _index = OsmTiledDbOperations.LoadIndex(this.Path);
        }

        /// <inheritdoc/>
        public override async Task<OsmGeo?> Get(OsmGeoType type, long id)
        {
            var pointer = _index.Get((new OsmGeoKey(type, id)));
            if (!pointer.HasValue) return null;
            
            return _data.Get(pointer.Value);
        }

        /// <inheritdoc/>
        public override async Task<IEnumerable<(uint x, uint y)>> GetTiles(OsmGeoType type, long id)
        {  
            var pointer = _index.Get((new OsmGeoKey(type, id)));
            if (!pointer.HasValue) return Enumerable.Empty<(uint x, uint y)>();

            var tiles = new List<(uint x, uint y)>();
            
            foreach (var tileId in _data.GetTilesFor(pointer.Value))
            {
                tiles.Add(Tile.FromLocalId(this.Zoom, tileId));
            }

            return tiles;
        }

        /// <inheritdoc/>
        public override async Task<IEnumerable<OsmGeo>> Get((uint x, uint y) tile)
        {
            var tileId = Tile.ToLocalId(tile, this.Zoom);

            return _data.GetForTile(tileId);
        }
    }
}