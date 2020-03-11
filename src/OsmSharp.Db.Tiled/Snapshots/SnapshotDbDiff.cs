using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a diff relative to another snapshot.
    /// </summary>
    public class SnapshotDbDiff : SnapshotDb
    {
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>> _nodeIndexesCache;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>> _wayIndexesCache;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        public SnapshotDbDiff(string path)
            : base(path)
        {
            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>>();
        }

        internal SnapshotDbDiff(string path, SnapshotDbMeta meta)
            : base(path, meta)
        {
            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, DeletedIndex>>();
        }

        /// <inheritdoc/>
        internal override OsmGeo Get(OsmGeoType type, long id, Func<Tile, bool> isDeleted)
        {
            var local = this.GetLocal(type, id);
            if (local.deleted) return null;
            if (local.osmGeo != null) return local.osmGeo;

            // move to base.
            var b = this.GetBaseDb();
            
            // get from base but take into account deleted objects.
            return b.Get(type, id,(tile) =>
            {
                var deletedIndex = LoadDeletedIndex(tile, type);

                if (deletedIndex != null &&
                    deletedIndex.Contains(id))
                {
                    return true;
                }

                return false;
            });
        }
        
        private DeletedIndex LoadDeletedIndex(Tile tile, OsmGeoType type, bool create = false)
        {
            if (type == OsmGeoType.Node)
            {
                if (!_nodeIndexesCache.TryGetValue(tile.Zoom, out var cached))
                {
                    cached = new LRUCache<ulong, DeletedIndex>(10);
                    _nodeIndexesCache[tile.Zoom] = cached;
                }

                if (cached.TryGetValue(tile.LocalId, out var index))
                {
                    if (index == null && create)
                    {
                        index = new DeletedIndex();
                        cached.Add(tile.LocalId, index);
                    }
                    return index;
                }

                index = SnapshotDbOperations.LoadDeletedIndex(this.Path, tile, type);
                if (create && index == null)
                {
                    index = new DeletedIndex();
                }
                cached.Add(tile.LocalId, index);
                return index;
            }
            else
            {
                if (!_wayIndexesCache.TryGetValue(tile.Zoom, out var cached))
                {
                    cached = new LRUCache<ulong, DeletedIndex>(10);
                    _wayIndexesCache[tile.Zoom] = cached;
                }

                if (cached.TryGetValue(tile.LocalId, out var index))
                {
                    if (index == null && create)
                    {
                        index = new DeletedIndex();
                        cached.Add(tile.LocalId, index);
                    }
                    return index;
                }

                index = SnapshotDbOperations.LoadDeletedIndex(this.Path, tile, type);
                if (create && index == null)
                {
                    index = new DeletedIndex();
                }
                cached.Add(tile.LocalId, index);
                return index;
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(uint x, uint y, OsmGeoType type)
        {
            var tile = new Tile(x, y, this.Zoom);
            var updateTile = this.GetCreatedOrModifiedTile(tile, type);

            return updateTile?.Where(geo =>
            {
                var deletedIndex = LoadDeletedIndex(tile, type);

                if (deletedIndex != null &&
                    deletedIndex.Contains(geo.Id.Value))
                {
                    return false;
                }

                return true;
            });
        }

        private IEnumerable<OsmGeo> GetCreatedOrModifiedTile(Tile tile, OsmGeoType type)
        {
            // get local data.
            var localData = SnapshotDbOperations.GetLocalTile(this.Path, this.Zoom, tile, type);

            // get base data.
            var b = this.GetBaseDb();
            var baseData = b.GetTile(tile.X, tile.Y, type);

            // merge or return.
            if (localData != null && baseData != null)
            {
                return baseData.Merge(localData);
            }
            if (baseData != null)
            {
                return baseData;
            }
            return localData;
        }

        /// <inheritdoc/>
        public override IEnumerable<Tile> GetChangedTiles()
        {
            return SnapshotDbOperations.GetTiles(this.Path, this.Zoom);
        }

        /// <inheritdoc/>
        internal override IEnumerable<Tile> GetChangedTilesSinceLatestDiff()
        {
            // get base data.
            var b = this.GetBaseDb();
            var baseTiles = b.GetChangedTilesSinceLatestDiff();
            
            // gets the local tiles.
            var localTiles = SnapshotDbOperations.GetTiles(this.Path, this.Zoom);
            
            var tiles = new HashSet<Tile>();

            foreach (var tile in baseTiles)
            {
                tiles.Add(tile);
            }

            foreach (var tile in localTiles)
            {
                tiles.Add(tile);
            }

            return tiles;
        }

        /// <inheritdoc/>
        internal override IEnumerable<Tile> GetIndexesForZoom(uint zoom)
        {            
            // get base data.
            var b = this.GetBaseDb();
            var baseTiles = b.GetIndexesForZoom(zoom);
            
            // gets the local tiles.
            var localTiles = SnapshotDbOperations.GetIndexTiles(this.Path, zoom);
            
            return baseTiles.Merge(localTiles);
        }

        /// <inheritdoc/>
        internal override IEnumerable<(long id, int mask)> GetSortedIndexData(Tile tile, OsmGeoType type)
        {
            var index = this.LoadIndex(tile, type);

            var baseIndex = this.GetBaseDb().LoadIndex(tile, type);

            return baseIndex.Merge(index, (t1, t2) => t1.id.CompareTo(t2.id));
        }

        /// <inheritdoc/>
        internal override SnapshotDb GetLatestNonDiff()
        {
            return this.GetBaseDb().GetLatestNonDiff();
        }
    }
}