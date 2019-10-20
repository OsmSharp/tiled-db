using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;
using Serilog;

namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary>
    /// Represents a snapshot of OSM data at a given point in time.
    /// </summary>
    public abstract class SnapshotDb
    {
        private readonly string _path;
        private readonly SnapshotDbMeta _meta;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _nodeIndexesCache;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _wayIndexesCache;

        protected SnapshotDb(string path)
        {
            _path = path;

            _meta = SnapshotDbOperations.LoadDbMeta(_path);
            
            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
        }

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        internal uint Zoom => _meta.Zoom;
        
        /// <summary>
        /// Gets the object with the given type and id.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The object.</returns>
        public abstract OsmGeo Get(OsmGeoType type, long id);
        
        /// <summary>
        /// Gets the data in the given tile.
        /// </summary>
        /// <param name="tile">The tile to get the data for.</param>
        /// <param name="type">The type to get the data for.</param>
        /// <returns>The data in the given tile.</returns>
        public abstract IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type);
        
        /// <summary>
        /// Loads an index for the given type and tile and optionally creates it.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <param name="type">The type.</param>
        /// <param name="create">Flag to create when index doesn't exist.</param>
        /// <returns>The index.</returns>
        protected Index LoadIndex(Tile tile, OsmGeoType type, bool create = false)
        {
            if (type == OsmGeoType.Node)
            {
                if (!_nodeIndexesCache.TryGetValue(tile.Zoom, out var cached))
                {
                    cached = new LRUCache<ulong, Index>(10);
                    _nodeIndexesCache[tile.Zoom] = cached;
                }

                if (cached.TryGetValue(tile.LocalId, out var index))
                {
                    if (index == null && create)
                    {
                        index = new Index();
                        cached.Add(tile.LocalId, index);
                    }
                    return index;
                }

                index = SnapshotDbOperations.LoadIndex(_path, tile, type);
                if (create && index == null)
                {
                    index = new Index();
                }
                cached.Add(tile.LocalId, index);
                return index;
            }
            else
            {
                if (!_wayIndexesCache.TryGetValue(tile.Zoom, out var cached))
                {
                    cached = new LRUCache<ulong, Index>(10);
                    _wayIndexesCache[tile.Zoom] = cached;
                }

                if (cached.TryGetValue(tile.LocalId, out var index))
                {
                    if (index == null && create)
                    {
                        index = new Index();
                        cached.Add(tile.LocalId, index);
                    }
                    return index;
                }

                index = SnapshotDbOperations.LoadIndex(_path, tile, type);
                if (create && index == null)
                {
                    index = new Index();
                }
                cached.Add(tile.LocalId, index);
                return index;
            }
        }
        
        /// <summary>
        /// Gets a local tile, returns null if non found.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        protected OsmGeo GetLocal(OsmGeoType type, long id)
        {
            var tile = new Tile(0, 0, 0);
            var index = LoadIndex(tile, type);

            while (index != null &&
                   index.TryGetMask(id, out var mask))
            {
                var subTiles = tile.SubTilesForMask2(mask);
                var subTile = subTiles.First();

                if (subTile.Zoom == _meta.Zoom)
                { // load data and find object.
                    var stream = SnapshotDbOperations.LoadTile(_path, type, subTile);
                    if (stream == null)
                    {
                        Log.Warning($"Could not find sub tile, it should be there: {subTile}");
                        return null;
                    }
                    using (stream)
                    {
                        var source = new OsmSharp.Streams.BinaryOsmStreamSource(stream);
                        while (source.MoveNext())
                        {
                            var current = source.Current();

                            if (current.Id == id)
                            {
                                return current;
                            }
                        }
                    }
                }

                tile = subTile;
                index = LoadIndex(tile, type);
            }

            return null;
        }

        /// <summary>
        /// Returns an enumerable containing all objects of a given type for the given tile that are store locally.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <param name="type">The type.</param>
        /// <returns>An enumerable containing all objects of a given type for the given tile</returns>
        protected IEnumerable<OsmGeo> GetLocalTile(Tile tile, OsmGeoType type)
        {
            // TODO: dispose the returned streams, implement this in OSM stream source.
            if (tile.Zoom != this.Zoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");

            var dataTile = SnapshotDbOperations.LoadTile(this._path, type, tile);
            if (dataTile == null) yield break;

            using (dataTile)
            {
                foreach (var osmGeo in new Streams.BinaryOsmStreamSource(dataTile))
                {
                    yield return osmGeo;
                }
            }
        }
    }
}