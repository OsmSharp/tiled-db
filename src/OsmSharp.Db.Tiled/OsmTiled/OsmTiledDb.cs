using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.OsmTiled.Data;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class OsmTiledDb : OsmTiledDbBase, IDisposable
    {
        private readonly OsmTiledDbOsmGeoIndex _osmGeoIndex;
        private readonly OsmTiledLinkedStream _data;
        private readonly IOsmTiledDbTileIndexReadOnly _tileIndex;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        internal OsmTiledDb(string path)
            : base(path)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _osmGeoIndex = OsmTiledDbOperations.LoadOsmGeoIndex(this.Path);
            _tileIndex = OsmTiledDbOperations.LoadTileIndex(this.Path);
        }

        internal OsmTiledDb(string path, OsmTiledDbMeta meta)
            : base(path, meta)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _osmGeoIndex = OsmTiledDbOperations.LoadOsmGeoIndex(this.Path);
            _tileIndex = OsmTiledDbOperations.LoadTileIndex(this.Path);
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false)
        {
            foreach (var tileId in _tileIndex.GetTiles())
            {
                yield return Tile.FromLocalId(this.Zoom, tileId);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            if (osmGeoKeys == null)
            {
                foreach (var (osmGeo, tiles) in this._data.Get(buffer))
                {
                    yield return (osmGeo, tiles.Select((t) => Tile.FromLocalId(this.Zoom, t)));
                }
            }
            else
            {
                foreach (var osmGeoKey in osmGeoKeys)
                {
                    var pointer = _osmGeoIndex.Get(osmGeoKey);
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
                var pointer = _osmGeoIndex.Get(osmGeoKey);
                if (pointer == null) continue;

                var tiles = _data.GetTilesFor(pointer.Value)
                    .Select(x => Tile.FromLocalId(this.Zoom, x));
                yield return (osmGeoKey, tiles);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            
            var lowestTilePointer = _tileIndex.LowestPointerFor(tiles.Select(x => Tile.ToLocalId(x, this.Zoom)));
            if (lowestTilePointer == null) yield break;
            
            if (tiles.HasOne(out var only))
            {
                var tileId = Tile.ToLocalId(only, this.Zoom);
                foreach (var osmGeo in _data.GetForTile(lowestTilePointer.Value, tileId, buffer))
                {
                    yield return (osmGeo, tiles);
                }
            }
            else
            {
                foreach (var (osmGeo, osmGeoTiles) in _data.GetForTiles(lowestTilePointer.Value, tiles.Select(x => Tile.ToLocalId(x, this.Zoom)), 
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