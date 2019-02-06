using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Changesets;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using Serilog;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Abstract base class for all databases.
    /// </summary>
    public abstract class DatabaseBase : IDatabaseView
    {
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _nodeIndexesCache;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _wayIndexesCache;
        private readonly string _path;
        private readonly bool _mapped;
        private readonly uint _zoom;
        private readonly bool _compressed;
        
        protected DatabaseBase(string path, DatabaseMeta meta, bool mapped)
        {
            _path = path;
            _mapped = mapped;
            _zoom = meta.Zoom;
            _compressed = meta.Compressed;
            
            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();

            // save meta if needed.
            var metaPath = DatabaseCommon.PathToMeta(_path);
            if (FileSystemFacade.FileSystem.Exists(metaPath)) return;
            using (var stream = FileSystemFacade.FileSystem.Open(metaPath, FileMode.Create))
            {
                meta.Serialize(stream);
            }
        }

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        public uint Zoom => _zoom;

        /// <summary>
        /// Gets the compressed flag.
        /// </summary>
        public bool Compressed => _compressed;

        /// <summary>
        /// Gets the path.
        /// </summary>
        public string Path => _path;

        /// <inheritdoc/>
        public abstract OsmGeo Get(OsmGeoType type, long id);

        /// <inheritdoc/>
        public abstract IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type);

        /// <inheritdoc/>
        public virtual IDatabaseView ApplyChangeset(OsmChange changeset, string path = null)
        {
            return DiffBuilder.Build(this, changeset, path);
        }
        
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
                    return index;
                }

                index = DatabaseCommon.LoadIndex(_path, tile, type, _mapped);
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
                    return index;
                }

                index = DatabaseCommon.LoadIndex(_path, tile, type, _mapped);
                if (create && index == null)
                {
                    index = new Index();
                }
                cached.Add(tile.LocalId, index);
                return index;
            }
        }
        
        protected void SaveIndex(Tile tile, OsmGeoType type, Index index)
        {
            DatabaseCommon.SaveIndex(_path, tile, type, index);
        }

        protected OsmGeo GetLocal(OsmGeoType type, long id)
        {
            var tile = new Tile(0, 0, 0);
            var index = LoadIndex(tile, type);

            while (index != null &&
                   index.TryGetMask(id, out var mask))
            {
                var subTiles = tile.SubTilesForMask2(mask);
                var subTile = subTiles.First();

                if (subTile.Zoom == _zoom)
                { // load data and find object.
                    var stream = DatabaseCommon.LoadTile(_path, type, subTile, _compressed);
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
        /// Gets the tiles for the given lat/lon.
        /// </summary>
        /// <param name="longitude">The longitude.</param>
        /// <param name="latitude">The latitude.</param>
        /// <returns>All tiles that overlap the given location.</returns>
        internal IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(double longitude, double latitude)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
            var tile = new Tile(0, 0, 0);
            uint zoom = 0;
            while (zoom < this.Zoom)
            {
                // determine next tile.
                var nextTile = Tiles.Tile.WorldToTileIndex(latitude, longitude, zoom + 2);
                
                // build mask and add previous tile.
                var mask = nextTile.BuildMask2();
                tilesPerZoom.Add(new[] {(tile, mask)});
                
                // move to next tile.
                tile = nextTile;
                zoom += 2;
            }
            
            // add the last tile with a don't-care mask.
            tilesPerZoom.Add(new [] { (Tile.WorldToTileIndex(latitude, longitude, this.Zoom), -1) });

            return tilesPerZoom;
        }

        /// <summary>
        /// Gets the tiles for the given objects.
        /// </summary>
        /// <param name="objects">The objects.</param>
        /// <returns>All tiles with the given object.</returns>
        internal virtual IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(IEnumerable<(OsmGeoType type, long id)> objects)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
            
            // do zoom level '0'.
            var tile = new Tile(0, 0, 0);
            var mask = 0;
            Index nodeIndex = null;
            Index wayIndex = null;
            Index relationIndex = null;
            foreach (var (type, id) in objects)
            {
                switch (type)
                {
                    case OsmGeoType.Node:
                        if (nodeIndex == null) nodeIndex = LoadIndex(tile, type);
                        if (nodeIndex != null &&
                            nodeIndex.TryGetMask(id, out var nodeMask))
                        {
                            mask |= nodeMask;
                        }

                        break;
                    case OsmGeoType.Way:
                        if (wayIndex == null) wayIndex = LoadIndex(tile, type);
                        if (wayIndex != null &&
                            wayIndex.TryGetMask(id, out var wayMask))
                        {
                            mask |= wayMask;
                        }

                        break;
                    case OsmGeoType.Relation:
                        if (relationIndex == null) relationIndex = LoadIndex(tile, type);
                        if (relationIndex != null &&
                            relationIndex.TryGetMask(id, out var relationMask))
                        {
                            mask |= relationMask;
                        }

                        break;
                }
            }
            
            // add first tile.
            tilesPerZoom.Add(new []{ (tile, mask)});

            // split tiles per level.
            uint zoom = 0;
            while (zoom < this.Zoom)
            {
                // move one level down and collect all tiles and masks.
                var tilesAtZoom = new List<(Tile tile, int mask)>();
                foreach (var (tileAbove, maskAbove) in tilesPerZoom[tilesPerZoom.Count - 1])
                { // go over all tiles in zoom.
                    foreach (var currentTile in tileAbove.SubTilesForMask2(maskAbove))
                    { // go over all tiles that have at least one object at zoom+2.
                        
                        // determine mask for the tile above over all objects.
                        mask = 0;
                        nodeIndex = null;
                        wayIndex = null;
                        relationIndex = null;
                        foreach (var (type, id) in objects)
                        {
                            switch (type)
                            {
                                case OsmGeoType.Node:
                                    if (nodeIndex == null) nodeIndex = LoadIndex(currentTile, type);
                                    if (nodeIndex != null &&
                                        nodeIndex.TryGetMask(id, out var nodeMask))
                                    {
                                        mask |= nodeMask;
                                    }
                                    break;
                                case OsmGeoType.Way:
                                    if (wayIndex == null) wayIndex = LoadIndex(currentTile, type);
                                    if (wayIndex != null &&
                                        wayIndex.TryGetMask(id, out var wayMask))
                                    {
                                        mask |= wayMask;
                                    }
                                    break;
                                case OsmGeoType.Relation:
                                    if (relationIndex == null) relationIndex = LoadIndex(currentTile, type);
                                    if (relationIndex != null &&
                                        relationIndex.TryGetMask(id, out var relationMask))
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
        /// Returns an enumerable containing all objects of a given type for the given tile that are store locally.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <param name="type">The type.</param>
        /// <returns>An enumerable containing all objects of a given type for the given tile</returns>
        protected IEnumerable<OsmGeo> GetLocalTile(Tile tile, OsmGeoType type)
        {
            // TODO: dispose the returned streams, implement this in OSM stream source.
            if (tile.Zoom != this.Zoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");

            var dataTile = DatabaseCommon.LoadTile(this._path, type, tile, this.Compressed);
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