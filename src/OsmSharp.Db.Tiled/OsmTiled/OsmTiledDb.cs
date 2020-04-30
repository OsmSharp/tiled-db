using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class OsmTiledDb : OsmTiledDbBase, IDisposable
    {
        private readonly OsmTiledIndex _index;
        private readonly OsmTiledLinkedStream _data;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        internal OsmTiledDb(string path)
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
        public override IEnumerable<OsmGeo> Get(IReadOnlyCollection<OsmGeoKey> osmGeoKeys, byte[]? buffer = null)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                var pointer = _index.Get(osmGeoKey);
                if (pointer == null) continue;
            
                yield return _data.Get(pointer.Value, buffer);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y, OsmGeoKey key)> GetTiles(IReadOnlyCollection<OsmGeoKey> osmGeoKeys)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                var pointer = _index.Get(osmGeoKey);
                if (pointer == null) continue;
            
                foreach (var tileId in _data.GetTilesFor(pointer.Value))
                {
                    var tile = Tile.FromLocalId(this.Zoom, tileId);
                    yield return (tile.x, tile.y, osmGeoKey);
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IReadOnlyCollection<(uint x, uint y)> tiles)> Get(IReadOnlyCollection<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            
            if (tiles.Count == 1)
            {
                var tileId = Tile.ToLocalId(tiles.First(), this.Zoom);
                foreach (var osmGeo in _data.GetForTile(tileId, buffer))
                {
                    yield return (osmGeo, tiles);
                }
            }
            else
            {
                foreach (var (osmGeo, osmGeoTiles) in _data.GetForTiles(tiles.Select(x => Tile.ToLocalId(x, this.Zoom)), 
                    buffer))
                {
                    yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                }
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _data?.Dispose();
        }
    }
}