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
    /// Represents a snapshot of OSM data at a given point in time represented by a diff relative to a full copy of the data.
    /// </summary>
    public class OsmTiledDbSnapshot : OsmTiledDbBase, IDisposable
    {
        private readonly Func<long, OsmTiledDbBase> _getBaseDb;
        
        internal OsmTiledDbSnapshot(string path, Func<long, OsmTiledDbBase> getBaseDb, OsmTiledDbMeta? meta = null)
            : base(path, meta ?? OsmTiledDbOperations.LoadDbMeta(path))
        {
            _getBaseDb = getBaseDb;
        }

        private OsmTiledDbOsmGeoIndex? _osmGeoIndex;
        private OsmTiledDbOsmGeoIndex OsmGeoIndex
        {
            get
            {
                return _osmGeoIndex ??= OsmTiledDbOperations.LoadOsmGeoIndex(this.Path);
            }
        }

        private IOsmTiledDbTileIndexReadOnly? _tileIndex;
        private IOsmTiledDbTileIndexReadOnly TileIndex
        {
            get
            {
                return _tileIndex ??= OsmTiledDbOperations.LoadTileIndex(this.Path);
            }
        }

        private OsmTiledLinkedStream? _data;
        private OsmTiledLinkedStream Data
        {
            get
            {
                return _data ??= OsmTiledDbOperations.LoadData(this.Path);
            }
        }

        private OsmTiledDbBase? _baseDb;
        
        /// <summary>
        /// Gets the base db.
        /// </summary>
        /// <returns>The base database this snapshot depends on.</returns>
        /// <exception cref="Exception"></exception>
        public  OsmTiledDbBase GetBaseDb()
        {
            if (this.Base == null) throw new Exception($"A {nameof(OsmTiledDbSnapshot)} always needs a base db.");
            return _baseDb ??= _getBaseDb(this.Base.Value);
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false)
        {
            if (modifiedOnly)
            {
                foreach (var tileId in this.TileIndex.GetTiles())
                {
                    yield return Tile.FromLocalId(this.Zoom, tileId);
                }
            }
            else
            {
                var baseTiles = this.GetBaseDb().GetTiles().Select(x => Tile.ToLocalId(x.x, x.y, this.Zoom));
                foreach (var tileId in baseTiles.MergeWhenSorted(this.TileIndex.GetTiles()))
                {
                    yield return Tile.FromLocalId(this.Zoom, tileId);
                }
            }
        }
        
        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            
            if (osmGeoKeys == null)
            {
                var allHere = this.Data.Get(buffer)
                    .Select(x => (x.osmGeo, x.tile
                        .Select(t => Tile.FromLocalId(this.Zoom, t))));
                var allBase = this.GetBaseDb().Get((IEnumerable<OsmGeoKey>?)null, buffer);
                var merged = allBase.MergeWhenSorted(allHere, (o1, o2) => 
                    (new OsmGeoKey(o1.osmGeo).CompareTo(new OsmGeoKey(o2.osmGeo))));
                foreach (var merge in merged)
                {
                    yield return merge;
                }
                yield break;
            }
            
            var index = this.OsmGeoIndex;
            foreach (var osmGeoKey in osmGeoKeys)
            {            
                var pointer = index.Get(osmGeoKey);
                if (pointer < 0) continue;

                if (pointer == null)
                {
                    var baseData = this.GetBaseDb().Get(osmGeoKey, buffer);
                    if (baseData == null) continue;
                    yield return baseData.Value;
                }
                else
                {
                    yield return (this.Data.Get(pointer.Value, buffer), 
                        this.Data.GetTilesFor(pointer.Value).Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeoKey key, IEnumerable<(uint x, uint y)> tiles)> GetTilesFor(IEnumerable<OsmGeoKey> osmGeoKeys)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                yield return (osmGeoKey, this.GetTilesFor(osmGeoKey));
            }
        }

        private IEnumerable<(uint x, uint y)> GetTilesFor(OsmGeoKey key)
        {  
            var pointer = this.OsmGeoIndex.Get(key);
            switch (pointer)
            {
                case -1:
                    // object was deleted!
                    yield break;
                case null:
                {
                    // attempt getting data from base db.
                    foreach (var osmGeo in this.GetBaseDb().GetTilesFor(new [] { key }))
                    {
                        foreach (var tile in osmGeo.tiles)
                        {
                            yield return tile;
                        }
                    }

                    break;
                }
                default:
                {
                    // object is here, get it locally.
                    foreach (var tileId in this.Data.GetTilesFor(pointer.Value))
                    {
                        yield return Tile.FromLocalId(this.Zoom, tileId);
                    }

                    break;
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            
            var data = this.Data;
            if (tiles.HasOne(out var only))
            {
                var tileId = Tile.ToLocalId(only, this.Zoom);
                var tileIsLocal = this.TileIndex.HasTile(tileId);
                if (tileIsLocal)
                {
                    var lowestTilePointer = this.TileIndex.LowestPointerFor(tiles.Select(x => Tile.ToLocalId(x, this.Zoom)));
                    if (lowestTilePointer == null) yield break;
                    
                    // tile is in this diff.
                    foreach (var osmGeo in data.GetForTile(lowestTilePointer.Value, tileId, buffer))
                    {
                        yield return (osmGeo, tiles);
                    }
                }
                else
                {
                    // tile is not in diff.
                    foreach (var t in this.GetBaseDb().Get(tiles, buffer))
                    {
                        yield return t;
                    }
                }
            }
            else
            {
                var localTiles = new List<(uint x, uint y)>();
                var otherTiles = new List<(uint x, uint y)>();
                foreach (var tile in tiles)
                {
                    if (this.TileIndex.HasTile(Tile.ToLocalId(tile, this.Zoom)))
                    {
                        localTiles.Add(tile);
                    }
                    else
                    {
                        otherTiles.Add(tile);
                    }
                }

                if (localTiles.Count == 0)
                {
                    // no tiles is in this diff.
                    foreach (var t in this.GetBaseDb().Get(otherTiles, buffer))
                    {
                        yield return t;
                    }

                    yield break;
                }

                var lowestTilePointer = this.TileIndex.LowestPointerFor(localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)));
                if (lowestTilePointer == null) yield break;

                if (otherTiles.Count == 0)
                {
                    // all tiles in this diff.
                    foreach (var (osmGeo, osmGeoTiles) in data.GetForTiles(lowestTilePointer.Value,
                        localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)), buffer))
                    {
                        yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                    }

                    yield break;
                }

                // data in both, merge the two.
                using var baseEnumerator = this.GetBaseDb().Get(otherTiles, buffer).GetEnumerator();
                using var thisEnumerator = data.GetForTiles(lowestTilePointer.Value, localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
                    buffer).GetEnumerator();
                var baseHasNext = baseEnumerator.MoveNext();
                var thisHasNext = thisEnumerator.MoveNext();

                while (baseHasNext || thisHasNext)
                {
                    if (baseHasNext && thisHasNext)
                    {
                        var baseKey = new OsmGeoKey(baseEnumerator.Current.osmGeo);
                        var thisKey = new OsmGeoKey(thisEnumerator.Current.osmGeo);

                        if (baseKey < thisKey)
                        {
                            yield return baseEnumerator.Current;
                            baseHasNext = baseEnumerator.MoveNext();
                        }
                        else if (thisKey < baseKey)
                        {
                            var (osmGeo, osmGeoTiles) = thisEnumerator.Current;
                            yield return (osmGeo,
                                osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                            thisHasNext = thisEnumerator.MoveNext();
                        }
                        else
                        {
                            var (osmGeo, osmGeoTiles) = thisEnumerator.Current;
                            yield return (osmGeo,
                                osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                            baseHasNext = baseEnumerator.MoveNext();
                            thisHasNext = thisEnumerator.MoveNext();
                        }
                    }
                    else if (baseHasNext)
                    {
                        yield return baseEnumerator.Current;
                        baseHasNext = baseEnumerator.MoveNext();
                    }
                    else
                    {
                        var (osmGeo, osmGeoTiles) = thisEnumerator.Current;
                        yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                        thisHasNext = thisEnumerator.MoveNext();
                    }
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