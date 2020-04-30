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
        private readonly OsmTiledIndex _index;
        private readonly OsmTiledLinkedStream _data;
        private readonly OsmTiledDbBase _baseDb;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        internal OsmTiledDbSnapshot(string path, OsmTiledDbBase? baseDb = null)
            : base(path)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _index = OsmTiledDbOperations.LoadIndex(this.Path);

            if (baseDb != null)
            {
                _baseDb = baseDb;
                return;
            }
            
            if (this.Base == null) throw new Exception("Cannot instantiate diff db without a the base database.");
            _baseDb = OsmTiledDbOperations.LoadDb(this.Base);
        }

        internal OsmTiledDbSnapshot(string path, OsmTiledDbMeta meta, OsmTiledDbBase? baseDb = null)
            : base(path, meta)
        {
            _data = OsmTiledDbOperations.LoadData(this.Path);
            _index = OsmTiledDbOperations.LoadIndex(this.Path);
            
            if (baseDb != null)
            {
                _baseDb = baseDb;
                return;
            }
            
            if (this.Base == null) throw new Exception("Cannot instantiate diff db without a the base database.");
            _baseDb = OsmTiledDbOperations.LoadDb(this.Base);
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
            var pointer = _index.Get(key);
            return pointer switch
            {
                // object was deleted!
                -1 => null,
                // attempt base db.
                null => _baseDb.Get(key, buffer),
                // get data locally.
                _ => _data.Get(pointer.Value, buffer)
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
            var pointer = _index.Get(key);
            switch (pointer)
            {
                case -1:
                    // object was deleted!
                    yield break;
                case null:
                {
                    // attempt getting data from base db.
                    foreach (var osmGeo in _baseDb.GetTiles(key))
                    {
                        yield return osmGeo;
                    }

                    break;
                }
                default:
                {
                    // object is here, get it locally.
                    foreach (var tileId in _data.GetTilesFor(pointer.Value))
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
            
            if (tiles.Count == 1)
            {
                var tileId = Tile.ToLocalId(tiles.First(), this.Zoom);
                var tileIsLocal = _data.HasTile(tileId);
                if (tileIsLocal)
                {
                    // tile is in this diff.
                    foreach (var osmGeo in _data.GetForTile(tileId, buffer))
                    {
                        yield return (osmGeo, tiles);
                    }
                }
                else
                {
                    // tile is not in diff.
                    foreach (var t in _baseDb.Get(tiles, buffer))
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
                    if (_data.HasTile(Tile.ToLocalId(tile, this.Zoom)))
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
                    foreach (var t in _baseDb.Get(otherTiles, buffer))
                    {
                        yield return t;
                    }

                    yield break;
                }


                if (otherTiles.Count == 0)
                {
                    // all tiles in this diff.
                    foreach (var (osmGeo, osmGeoTiles) in _data.GetForTiles(
                        localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
                        buffer))
                    {
                        yield return (osmGeo, osmGeoTiles.Select(x => Tile.FromLocalId(this.Zoom, x)).ToArray());
                    }

                    yield break;
                }

                // data in both, merge the two.
                using var baseEnumerator = _baseDb.Get(otherTiles, buffer).GetEnumerator();
                using var thisEnumerator = _data.GetForTiles(localTiles.Select(x => Tile.ToLocalId(x, this.Zoom)),
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