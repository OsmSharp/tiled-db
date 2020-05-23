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
    public class OsmTiledDb : OsmTiledDbBase
    {
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        internal OsmTiledDb(string path)
            : base(path)
        {
            
        }

        internal OsmTiledDb(string path, OsmTiledDbMeta meta)
            : base(path, meta)
        {
            
        }
        
        private OsmTiledDbOsmGeoIndex GetOsmGeoIndex() => OsmTiledDbOperations.LoadOsmGeoIndex(this.Path);
        private IOsmTiledDbTileIndexReadOnly GetTileIndex() => OsmTiledDbOperations.LoadTileIndex(this.Path);
        private OsmTiledLinkedStream GetData() => OsmTiledDbOperations.LoadData(this.Path);

        /// <inheritdoc/>
        public override OsmTiledDbBase? GetDbForTile((uint x, uint y) tile)
        {
            return this;
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false)
        {
            using var tileIndex = this.GetTileIndex();
            
            foreach (var tileId in tileIndex.GetTiles())
            {
                yield return Tile.FromLocalId(this.Zoom, tileId);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            using var data = this.GetData();
            
            if (osmGeoKeys == null)
            {
                foreach (var (osmGeo, tiles) in data.Get(buffer))
                {
                    yield return (osmGeo, tiles.Select((t) => Tile.FromLocalId(this.Zoom, t)));
                }
            }
            else
            {
                using var osmGeoIndex = this.GetOsmGeoIndex();
                var pointers = osmGeoIndex.GetAll(osmGeoKeys).Select(x => x.pointer);
                foreach (var osmGeoAndTiles in data.Get(pointers, buffer))
                {
                    yield return (osmGeoAndTiles.osmGeo,
                        osmGeoAndTiles.tiles.Select(x => Tile.FromLocalId(this.Zoom, x)));
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeoKey key, IEnumerable<(uint x, uint y)> tiles)> GetTilesFor(IEnumerable<OsmGeoKey> osmGeoKeys)
        {
            using var data = this.GetData();
            using var osmGeoIndex = this.GetOsmGeoIndex();
            
            foreach (var osmGeoKey in osmGeoKeys)
            {
                var pointer = osmGeoIndex.Get(osmGeoKey);
                if (pointer == null) continue;

                var tiles = data.GetTilesFor(pointer.Value)
                    .Select(x => Tile.FromLocalId(this.Zoom, x));
                yield return (osmGeoKey, tiles);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(
            byte[]? buffer = null)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            using var tileIndex = this.GetTileIndex();
            using var data = this.GetData();
            
            var lowestTilePointers = tileIndex.LowestPointersFor(tiles.Select(x => Tile.ToLocalId(x, this.Zoom))).ToList();
            if (lowestTilePointers.Count == 0) yield break;
            
            if (tiles.HasOne(out var only))
            {
                var tileId = Tile.ToLocalId(only, this.Zoom);
                foreach (var osmGeo in data.GetForTile(lowestTilePointers[0], tileId, buffer))
                {
                    yield return (osmGeo, tiles);
                }
            }
            else
            {
                foreach (var (osmGeo, osmGeoTiles) in data.GetForTiles(lowestTilePointers, tiles.Select(x => Tile.ToLocalId(x, this.Zoom)), 
                    buffer))
                {
                    yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                }
            }
        }
    }
}