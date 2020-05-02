using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
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

        private OsmTiledIndex? _index;
        private OsmTiledIndex GetIndex()
        {
            return _index ??= OsmTiledDbOperations.LoadIndex(this.Path);
        }

        private OsmTiledLinkedStream? _data;
        private OsmTiledLinkedStream GetData()
        {
            return _data ??= OsmTiledDbOperations.LoadData(this.Path);
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
                foreach (var tileId in this.GetData().GetTiles())
                {
                    yield return Tile.FromLocalId(this.Zoom, tileId);
                }
            }
            else
            {
                var baseTiles = this.GetBaseDb().GetTiles().Select(x => Tile.ToLocalId(x.x, x.y, this.Zoom));
                foreach (var tileId in baseTiles.MergeWhenSorted(this.GetData().GetTiles()))
                {
                    yield return Tile.FromLocalId(this.Zoom, tileId);
                }
            }
        }
        
        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys = null)
        {
            var buffer = new byte[1024];
            
            if (osmGeoKeys == null)
            {
                var allHere = this.GetData().Get(buffer)
                    .Select(x => (x.osmGeo, x.tile
                        .Select(t => Tile.FromLocalId(this.Zoom, t))));
                var allBase = this.GetBaseDb().Get();
                var merged = allBase.MergeWhenSorted(allHere, (o1, o2) => 
                    (new OsmGeoKey(o1.osmGeo).CompareTo(new OsmGeoKey(o2.osmGeo))));
                foreach (var merge in merged)
                {
                    yield return merge;
                }
                yield break;
            }
            
            var index = this.GetIndex();
            foreach (var osmGeoKey in osmGeoKeys)
            {            
                var pointer = index.Get(osmGeoKey);
                if (pointer < 0) continue;

                if (pointer == null)
                {
                    var baseData = this.GetBaseDb().Get(osmGeoKey);
                    if (baseData == null) continue;
                    yield return baseData.Value;
                }
                else
                {
                    yield return (this.GetData().Get(pointer.Value, buffer), 
                        this.GetData().GetTilesFor(pointer.Value).Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
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
            var pointer = this.GetIndex().Get(key);
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
                    foreach (var tileId in this.GetData().GetTilesFor(pointer.Value))
                    {
                        yield return Tile.FromLocalId(this.Zoom, tileId);
                    }

                    break;
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles)
        {
            var buffer = new byte[1024];
            
            var data = this.GetData();
            if (tiles.HasOne(out var only))
            {
                var tileId = Tile.ToLocalId(only, this.Zoom);
                var tileIsLocal = data.HasTile(tileId);
                if (tileIsLocal)
                {
                    // tile is in this diff.
                    foreach (var osmGeo in data.GetForTile(tileId, buffer))
                    {
                        yield return (osmGeo, tiles);
                    }
                }
                else
                {
                    // tile is not in diff.
                    foreach (var t in this.GetBaseDb().Get(tiles))
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
                    if (data.HasTile(Tile.ToLocalId(tile, this.Zoom)))
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
                    foreach (var t in this.GetBaseDb().Get(otherTiles))
                    {
                        yield return t;
                    }

                    yield break;
                }


                if (otherTiles.Count == 0)
                {
                    // all tiles in this diff.
                    foreach (var (osmGeo, osmGeoTiles) in data.GetForTiles(
                        localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
                        buffer))
                    {
                        yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                    }

                    yield break;
                }

                // data in both, merge the two.
                using var baseEnumerator = this.GetBaseDb().Get(otherTiles).GetEnumerator();
                using var thisEnumerator = data.GetForTiles(localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
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