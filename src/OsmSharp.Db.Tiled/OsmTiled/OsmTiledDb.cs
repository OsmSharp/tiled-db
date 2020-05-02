using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
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
        public override IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false)
        {
            foreach (var tileId in _data.GetTiles())
            {
                yield return Tile.FromLocalId(this.Zoom, tileId);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys = null)
        {
            var buffer = new byte[1024];
            if (osmGeoKeys == null)
            {
                
            }
            else
            {
                foreach (var osmGeoKey in osmGeoKeys)
                {
                    var pointer = _index.Get(osmGeoKey);
                    if (pointer == null) continue;
            
                    yield return (_data.Get(pointer.Value, buffer), 
                        _data.GetTilesFor(pointer.Value).Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeoKey key, IEnumerable<(uint x, uint y)> tiles)> GetTilesFor(IEnumerable<OsmGeoKey> osmGeoKeys)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                var pointer = _index.Get(osmGeoKey);
                if (pointer == null) continue;

                var tiles = _data.GetTilesFor(pointer.Value)
                    .Select(x => Tile.FromLocalId(this.Zoom, x));
                yield return (osmGeoKey, tiles);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles)
        {
            var buffer = new byte[1024];
            
            if (tiles.HasOne(out var only))
            {
                var tileId = Tile.ToLocalId(only, this.Zoom);
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