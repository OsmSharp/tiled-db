using System;
using System.Collections.Generic;
using System.Linq;
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
        private OsmTiledDbBase GetBaseDb()
        {
            if (this.Base == null) throw new Exception($"A {nameof(OsmTiledDbSnapshot)} always needs a base db.");
            return _baseDb ??= _getBaseDb(this.Base.Value);
        }
        
        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> Get(IReadOnlyCollection<OsmGeoKey> osmGeoKeys, byte[]? buffer = null)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                var osmGeo = this.Get(osmGeoKey, buffer);
                if (osmGeo == null) continue;
                
                yield return osmGeo;
            }
        }

        /// <inheritdoc/>
        public override OsmGeo? Get(OsmGeoKey key, byte[]? buffer = null)
        {
            var index = this.GetIndex();
            
            var pointer = index.Get(key);
            return pointer switch
            {
                // object was deleted!
                -1 => null,
                // attempt base db.
                null => this.GetBaseDb().Get(key, buffer),
                // get data locally.
                _ => this.GetData().Get(pointer.Value, buffer)
            };
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y, OsmGeoKey key)> GetTiles(IReadOnlyCollection<OsmGeoKey> osmGeoKeys)
        {
            foreach (var osmGeoKey in osmGeoKeys)
            {
                foreach (var (x, y) in this.GetTiles(osmGeoKey))
                {
                    yield return (x, y, osmGeoKey);
                }
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<(uint x, uint y)> GetTiles(OsmGeoKey key)
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
                    foreach (var osmGeo in this.GetBaseDb().GetTiles(key))
                    {
                        yield return osmGeo;
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
        public override IEnumerable<(OsmGeo osmGeo, IReadOnlyCollection<(uint x, uint y)> tiles)> Get(IReadOnlyCollection<(uint x, uint y)> tiles, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            var data = this.GetData();
            if (tiles.Count == 1)
            {
                var tileId = Tile.ToLocalId(tiles.First(), this.Zoom);
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
                    foreach (var t in this.GetBaseDb().Get(tiles, buffer))
                    {
                        yield return t;
                    }
                }
            }
            else
            {
                var localTiles = new List<(uint x, uint y)>(tiles.Count);
                var otherTiles = new List<(uint x, uint y)>(tiles.Count);
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
                    foreach (var t in this.GetBaseDb().Get(otherTiles, buffer))
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
                using var baseEnumerator = this.GetBaseDb().Get(otherTiles, buffer).GetEnumerator();
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