using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a diff of OSM data at a given point in time represented by a diff relative to a full copy of the data.
    /// </summary>
    public class OsmTiledDbDiff : OsmTiledDbBase, IDisposable
    {
        private readonly Func<long, OsmTiledDbBase> _getBaseDb;
        
        internal OsmTiledDbDiff(string path, Func<long, OsmTiledDbBase> getBaseDb, OsmTiledDbMeta? meta = null)
            : base(path, meta ?? OsmTiledDbOperations.LoadDbMeta(path))
        {
            _getBaseDb = getBaseDb;
        }

        private OsmTiledIndex? _index;
        private OsmTiledIndex Index
        {
            get
            {
                return _index ??= OsmTiledDbOperations.LoadIndex(this.Path);
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
        public OsmTiledDbBase GetBaseDb()
        {
            if (this.Base == null) throw new Exception($"A {nameof(OsmTiledDbSnapshot)} always needs a base db.");
            return _baseDb ??= _getBaseDb(this.Base.Value);
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false)
        {
            if (modifiedOnly)
            {
                foreach (var tileId in this.Data.GetTiles())
                {
                    yield return Tile.FromLocalId(this.Zoom, tileId);
                }
            }
            else
            {
                var baseTiles = this.GetBaseDb().GetTiles().Select(x => Tile.ToLocalId(x.x, x.y, this.Zoom));
                foreach (var tileId in baseTiles.MergeWhenSorted(this.Data.GetTiles()))
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
                var merged = allBase.ApplyTiledDiffStream(allBase);
                foreach (var merge in merged)
                {
                    yield return merge;
                }
                yield break;
            }

            var index = this.Index;
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
            var pointer = this.Index.Get(key);
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
        public override IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(
            IEnumerable<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];

            var data = this.Data;

            // always assume data in both, merge the two.
            using var baseEnumerator = this.GetBaseDb().Get(tiles, buffer).GetEnumerator();
            using var thisEnumerator = data.GetForTiles(tiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
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
                        if (!osmGeo.IsDeleted())
                        {
                            yield return (osmGeo,
                                osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                        }

                        thisHasNext = thisEnumerator.MoveNext();
                    }
                    else
                    {
                        var (osmGeo, osmGeoTiles) = thisEnumerator.Current;
                        if (!osmGeo.IsDeleted())
                        {
                            yield return (osmGeo,
                                osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                        }

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
                    if (!osmGeo.IsDeleted())
                    {
                        yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                    }

                    thisHasNext = thisEnumerator.MoveNext();
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