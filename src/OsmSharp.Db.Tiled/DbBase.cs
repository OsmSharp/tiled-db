//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Linq;
//using OsmSharp.Db.Tiled.Collections;
//using OsmSharp.Db.Tiled.Indexes;
//using OsmSharp.Db.Tiled.Meta;
//using OsmSharp.Db.Tiled.Tiles;
//using Serilog;
//
//namespace OsmSharp.Db.Tiled
//{
//    /// <summary>
//    /// Abstract base class for all databases.
//    /// </summary>
//    public abstract class DbBase
//    {
//        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _nodeIndexesCache;
//        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _wayIndexesCache;
//        private readonly string _path;
//        private readonly DbMeta _meta;
//        
//        protected DbBase(string path)
//        {
//            _path = path;
//
//            _meta = DbOperations.LoadDbMeta(_path);
//            
//            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
//            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
//        }
//
//        /// <summary>
//        /// Gets the zoom.
//        /// </summary>
//        public uint Zoom => _meta.Zoom;
//
//        /// <summary>
//        /// Gets the path.
//        /// </summary>
//        public string Path => _path;
//
//        /// <inheritdoc/>
//        public abstract OsmGeo Get(OsmGeoType type, long id);
//
//        /// <inheritdoc/>
//        public abstract IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type);
//
////        /// <inheritdoc/>
////        public virtual IDatabaseView ApplyChangeset(OsmChange changeset, string path = null)
////        {
////            return DiffBuilder.Build(this, changeset, path);
////        }
//        
//        protected Index LoadIndex(Tile tile, OsmGeoType type, bool create = false)
//        {
//            if (type == OsmGeoType.Node)
//            {
//                if (!_nodeIndexesCache.TryGetValue(tile.Zoom, out var cached))
//                {
//                    cached = new LRUCache<ulong, Index>(10);
//                    _nodeIndexesCache[tile.Zoom] = cached;
//                }
//
//                if (cached.TryGetValue(tile.LocalId, out var index))
//                {
//                    if (index == null && create)
//                    {
//                        index = new Index();
//                        cached.Add(tile.LocalId, index);
//                    }
//                    return index;
//                }
//
//                index = DbOperations.LoadIndex(_path, tile, type);
//                if (create && index == null)
//                {
//                    index = new Index();
//                }
//                cached.Add(tile.LocalId, index);
//                return index;
//            }
//            else
//            {
//                if (!_wayIndexesCache.TryGetValue(tile.Zoom, out var cached))
//                {
//                    cached = new LRUCache<ulong, Index>(10);
//                    _wayIndexesCache[tile.Zoom] = cached;
//                }
//
//                if (cached.TryGetValue(tile.LocalId, out var index))
//                {
//                    if (index == null && create)
//                    {
//                        index = new Index();
//                        cached.Add(tile.LocalId, index);
//                    }
//                    return index;
//                }
//
//                index = DbOperations.LoadIndex(_path, tile, type);
//                if (create && index == null)
//                {
//                    index = new Index();
//                }
//                cached.Add(tile.LocalId, index);
//                return index;
//            }
//        }
//        
//        protected void SaveIndex(Tile tile, OsmGeoType type, Index index)
//        {
//            DbOperations.SaveIndex(_path, tile, type, index);
//        }
//
//        protected OsmGeo GetLocal(OsmGeoType type, long id)
//        {
//            var tile = new Tile(0, 0, 0);
//            var index = LoadIndex(tile, type);
//
//            while (index != null &&
//                   index.TryGetMask(id, out var mask))
//            {
//                var subTiles = tile.SubTilesForMask2(mask);
//                var subTile = subTiles.First();
//
//                if (subTile.Zoom == _meta.Zoom)
//                { // load data and find object.
//                    var stream = DbOperations.LoadTile(_path, type, subTile);
//                    if (stream == null)
//                    {
//                        Log.Warning($"Could not find sub tile, it should be there: {subTile}");
//                        return null;
//                    }
//                    using (stream)
//                    {
//                        var source = new OsmSharp.Streams.BinaryOsmStreamSource(stream);
//                        while (source.MoveNext())
//                        {
//                            var current = source.Current();
//
//                            if (current.Id == id)
//                            {
//                                return current;
//                            }
//                        }
//                    }
//                }
//
//                tile = subTile;
//                index = LoadIndex(tile, type);
//            }
//
//            return null;
//        }
//
//        /// <summary>
//        /// Gets the tiles for the given lat/lon.
//        /// </summary>
//        /// <param name="longitude">The longitude.</param>
//        /// <param name="latitude">The latitude.</param>
//        /// <returns>All tiles that overlap the given location.</returns>
//        internal IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(double longitude, double latitude)
//        {
//            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
//            var tile = new Tile(0, 0, 0);
//            uint zoom = 0;
//            while (zoom < this.Zoom)
//            {
//                // determine next tile.
//                var nextTile = Tiles.Tile.WorldToTileIndex(latitude, longitude, zoom + 2);
//                
//                // build mask and add previous tile.
//                var mask = nextTile.BuildMask2();
//                tilesPerZoom.Add(new[] {(tile, mask)});
//                
//                // move to next tile.
//                tile = nextTile;
//                zoom += 2;
//            }
//            
//            // add the last tile with a don't-care mask.
//            tilesPerZoom.Add(new [] { (Tile.WorldToTileIndex(latitude, longitude, this.Zoom), -1) });
//
//            return tilesPerZoom;
//        }
//
//        /// <summary>
//        /// Gets the tiles for the given objects.
//        /// </summary>
//        /// <param name="objects">The objects.</param>
//        /// <returns>All tiles with the given object.</returns>
//        internal virtual IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(IEnumerable<(OsmGeoType type, long id)> objects)
//        {
//            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
//            
//            // do zoom level '0'.
//            var tile = new Tile(0, 0, 0);
//            var mask = 0;
//            Index nodeIndex = null;
//            Index wayIndex = null;
//            Index relationIndex = null;
//            foreach (var (type, id) in objects)
//            {
//                switch (type)
//                {
//                    case OsmGeoType.Node:
//                        if (nodeIndex == null) nodeIndex = LoadIndex(tile, type);
//                        if (nodeIndex != null &&
//                            nodeIndex.TryGetMask(id, out var nodeMask))
//                        {
//                            mask |= nodeMask;
//                        }
//
//                        break;
//                    case OsmGeoType.Way:
//                        if (wayIndex == null) wayIndex = LoadIndex(tile, type);
//                        if (wayIndex != null &&
//                            wayIndex.TryGetMask(id, out var wayMask))
//                        {
//                            mask |= wayMask;
//                        }
//
//                        break;
//                    case OsmGeoType.Relation:
//                        if (relationIndex == null) relationIndex = LoadIndex(tile, type);
//                        if (relationIndex != null &&
//                            relationIndex.TryGetMask(id, out var relationMask))
//                        {
//                            mask |= relationMask;
//                        }
//
//                        break;
//                }
//            }
//            
//            // add first tile.
//            tilesPerZoom.Add(new []{ (tile, mask)});
//
//            // split tiles per level.
//            uint zoom = 0;
//            while (zoom < this.Zoom)
//            {
//                // move one level down and collect all tiles and masks.
//                var tilesAtZoom = new List<(Tile tile, int mask)>();
//                foreach (var (tileAbove, maskAbove) in tilesPerZoom[tilesPerZoom.Count - 1])
//                { // go over all tiles in zoom.
//                    foreach (var currentTile in tileAbove.SubTilesForMask2(maskAbove))
//                    { // go over all tiles that have at least one object at zoom+2.
//                        
//                        // determine mask for the tile above over all objects.
//                        mask = 0;
//                        nodeIndex = null;
//                        wayIndex = null;
//                        relationIndex = null;
//                        foreach (var (type, id) in objects)
//                        {
//                            switch (type)
//                            {
//                                case OsmGeoType.Node:
//                                    if (nodeIndex == null) nodeIndex = LoadIndex(currentTile, type);
//                                    if (nodeIndex != null &&
//                                        nodeIndex.TryGetMask(id, out var nodeMask))
//                                    {
//                                        mask |= nodeMask;
//                                    }
//                                    break;
//                                case OsmGeoType.Way:
//                                    if (wayIndex == null) wayIndex = LoadIndex(currentTile, type);
//                                    if (wayIndex != null &&
//                                        wayIndex.TryGetMask(id, out var wayMask))
//                                    {
//                                        mask |= wayMask;
//                                    }
//                                    break;
//                                case OsmGeoType.Relation:
//                                    if (relationIndex == null) relationIndex = LoadIndex(currentTile, type);
//                                    if (relationIndex != null &&
//                                        relationIndex.TryGetMask(id, out var relationMask))
//                                    {
//                                        mask |= relationMask;
//                                    }
//                                    break;
//                            }
//                        }
//                        
//                        // log the current tile and its mask.
//                        tilesAtZoom.Add((currentTile, mask));
//                    }
//                }
//                
//                // keep what was collected.
//                tilesPerZoom.Add(tilesAtZoom);
//                
//                // move to the next level.
//                zoom += 2;
//            }
//
//            return tilesPerZoom;
//        }
//
//        /// <summary>
//        /// Returns an enumerable containing all objects of a given type for the given tile that are store locally.
//        /// </summary>
//        /// <param name="tile">The tile.</param>
//        /// <param name="type">The type.</param>
//        /// <returns>An enumerable containing all objects of a given type for the given tile</returns>
//        protected IEnumerable<OsmGeo> GetLocalTile(Tile tile, OsmGeoType type)
//        {
//            // TODO: dispose the returned streams, implement this in OSM stream source.
//            if (tile.Zoom != this.Zoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");
//
//            var dataTile = DbOperations.LoadTile(this._path, type, tile);
//            if (dataTile == null) yield break;
//
//            using (dataTile)
//            {
//                foreach (var osmGeo in new Streams.BinaryOsmStreamSource(dataTile))
//                {
//                    yield return osmGeo;
//                }
//            }
//        }
//    }
//}