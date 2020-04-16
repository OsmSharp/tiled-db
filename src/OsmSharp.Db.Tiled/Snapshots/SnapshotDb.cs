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
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, OsmGeoKeyIndex>> _indexesCache;

        protected SnapshotDb(string path)
            : this(path, SnapshotDbOperations.LoadDbMeta(path))
        {

        }

        protected SnapshotDb(string path, SnapshotDbMeta meta)
        {
            _path = path;
            _meta = meta;

            _indexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, OsmGeoKeyIndex>>();
        }

        /// <summary>
        /// Gets the path.
        /// </summary>
        internal string Path => _path;

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        public uint Zoom => _meta.Zoom;

        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public DateTime Timestamp => _meta.Timestamp;

        /// <summary>
        /// Gets the base.
        /// </summary>
        internal string Base => _meta.Base;

        /// <summary>
        /// Gets the object with the given type and id.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The object.</returns>
        public virtual OsmGeo Get(OsmGeoType type, long id)
        {
            return this.Get(type, id, null);
        }

        private SnapshotDb _baseDb = null;
        
        /// <summary>
        /// Gets the database this one is based on, if any.
        /// </summary>
        /// <returns></returns>
        internal virtual SnapshotDb GetBaseDb()
        {
            return _baseDb ??= SnapshotDbOperations.LoadDb(this.Base);
        }

        internal abstract OsmGeo Get(OsmGeoType type, long id, Func<Tile, bool> isDeleted);

        /// <summary>
        /// Gets the data in the given tile.
        /// </summary>
        /// <param name="x">The x coordinate of the tile to get the data for.</param>
        /// <param name="y">The y coordinate of the tile to get the data for.</param>
        /// <param name="type">The type to get the data for.</param>
        /// <returns>The data in the given tile.</returns>
        public abstract IEnumerable<OsmGeo> GetTile(uint x, uint y, OsmGeoType type);

        /// <summary>
        /// Loads an index for the given type and tile and optionally creates it.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <param name="create">Flag to create when index doesn't exist.</param>
        /// <returns>The index.</returns>
        internal OsmGeoKeyIndex LoadIndex(Tile tile, bool create = false)
        {
            if (!_indexesCache.TryGetValue(tile.Zoom, out var cached))
            {
                cached = new LRUCache<ulong, OsmGeoKeyIndex>(10);
                _indexesCache[tile.Zoom] = cached;
            }

            if (cached.TryGetValue(tile.LocalId, out var index))
            {
                if (index == null && create)
                {
                    index = new OsmGeoKeyIndex();
                    cached.Add(tile.LocalId, index);
                }

                return index;
            }

            index = SnapshotDbOperations.LoadIndex(_path, tile);
            if (create && index == null)
            {
                index = new OsmGeoKeyIndex();
            }

            cached.Add(tile.LocalId, index);
            return index;
        }

        protected (OsmGeo osmGeo, bool deleted) GetLocal(OsmGeoType type, long id, Func<Tile, bool> isDeleted = null)
        {
            var tile = new Tile(0, 0, 0);
            var index = LoadIndex(tile);

            while (index != null &&
                   index.TryGetMask(type, id, out var mask))
            {
                var subTiles = tile.SubTilesForMask2(mask);
                var subTile = subTiles.First();

                if (subTile.Zoom == _meta.Zoom)
                {
                    // load data and find object.
                    if (isDeleted != null && isDeleted(subTile)) return (null, true);

                    var stream = SnapshotDbOperations.LoadTile(_path, type, subTile);
                    if (stream == null)
                    {
                        Log.Warning($"Could not find sub tile, it should be there: {subTile}");
                        return (null, false);
                    }

                    using (stream)
                    {
                        var source = new OsmSharp.Streams.BinaryOsmStreamSource(stream);
                        while (source.MoveNext())
                        {
                            var current = source.Current();

                            if (current.Id == id)
                            {
                                return (current, false);
                            }
                        }
                    }
                }

                tile = subTile;
                index = LoadIndex(tile);
            }

            return (null, false);
        }

        /// <summary>
        /// Gets the tiles for the given objects.
        /// </summary>
        /// <param name="objects">The objects.</param>
        /// <returns>All tiles with the given object.</returns>
        internal virtual IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(
            IEnumerable<(OsmGeoType type, long id)> objects)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();

            // do zoom level '0'.
            var tile = new Tile(0, 0, 0);
            var mask = 0;
            foreach (var (type, id) in objects)
            {
                var index = LoadIndex(tile);
                
                switch (type)
                {
                    case OsmGeoType.Node:
                        if (index != null &&
                            index.TryGetMask(type, id, out var nodeMask))
                        {
                            mask |= nodeMask;
                        }

                        break;
                    case OsmGeoType.Way:
                        if (index != null &&
                            index.TryGetMask(type, id, out var wayMask))
                        {
                            mask |= wayMask;
                        }

                        break;
                    case OsmGeoType.Relation:
                        if (index != null &&
                            index.TryGetMask(type, id, out var relationMask))
                        {
                            mask |= relationMask;
                        }

                        break;
                }
            }

            // add first tile.
            tilesPerZoom.Add(new[] {(tile, mask)});

            // split tiles per level.
            uint zoom = 0;
            while (zoom < this.Zoom)
            {
                // move one level down and collect all tiles and masks.
                var tilesAtZoom = new List<(Tile tile, int mask)>();
                foreach (var (tileAbove, maskAbove) in tilesPerZoom[tilesPerZoom.Count - 1])
                {
                    // go over all tiles in zoom.
                    foreach (var currentTile in tileAbove.SubTilesForMask2(maskAbove))
                    {
                        // go over all tiles that have at least one object at zoom+2.

                        // determine mask for the tile above over all objects.
                        mask = 0;
                        foreach (var (type, id) in objects)
                        {
                            var index = LoadIndex(tile);
                            
                            switch (type)
                            {
                                case OsmGeoType.Node:
                                    if (index != null &&
                                        index.TryGetMask(type, id, out var nodeMask))
                                    {
                                        mask |= nodeMask;
                                    }

                                    break;
                                case OsmGeoType.Way:
                                    if (index != null &&
                                        index.TryGetMask(type, id, out var wayMask))
                                    {
                                        mask |= wayMask;
                                    }

                                    break;
                                case OsmGeoType.Relation:
                                    if (index != null &&
                                        index.TryGetMask(type, id, out var relationMask))
                                    {
                                        mask |= relationMask;
                                    }

                                    break;
                            }
                        }

                        // log the current tile and its mask.
                        tilesAtZoom.Add((currentTile, mask));
                    }
                }

                // keep what was collected.
                tilesPerZoom.Add(tilesAtZoom);

                // move to the next level.
                zoom += 2;
            }

            return tilesPerZoom;
        }

        /// <summary>
        /// Gets the changed tiles in this snapshot.
        /// </summary>
        /// <returns>All modified tiles.</returns>
        public abstract IEnumerable<Tile> GetChangedTiles();

        /// <summary>
        /// Gets the changed tiles since the latest non-diff.
        /// </summary>
        /// <returns>All modified tiles.</returns>
        internal abstract IEnumerable<Tile> GetChangedTilesSinceLatestDiff();

        /// <summary>
        /// Gets all the tiles with non-empty indexes for the given zoom level. 
        /// </summary>
        /// <param name="zoom">The zoom level.</param>
        /// <returns>All tiles with non-empty indexes.</returns>
        internal abstract IEnumerable<Tile> GetIndexesForZoom(uint zoom);

        /// <summary>
        /// Gets all masks for all the ids for the given index tile.
        /// </summary>
        /// <param name="tile">The tile.</param
        /// <returns>All the masks in the indexes.</returns>
        internal abstract IEnumerable<(OsmGeoType type, long id, int mask)> GetSortedIndexData(Tile tile);

        /// <summary>
        /// Gets the latest non-diff.
        /// </summary>
        /// <returns>The latest snapshot db that is not a diff.</returns>
        internal abstract SnapshotDb GetLatestNonDiff();
    }
}