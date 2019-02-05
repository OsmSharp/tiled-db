using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Tiles;
using Serilog;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Abstract base class for all databases.
    /// </summary>
    public abstract class DatabaseBase
    {
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _nodeIndexesCache;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _wayIndexesCache;
        private readonly string _path;
        private readonly bool _mapped;
        private readonly uint _zoom;
        private readonly bool _compressed;
        
        protected DatabaseBase(string path, bool mapped, uint zoom, bool compressed)
        {
            _path = path;
            _mapped = mapped;
            _zoom = zoom;
            _compressed = compressed;
            
            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
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

        /// <summary>
        /// Loads the index for the given type and tile.
        /// </summary>
        protected Index LoadIndex(OsmGeoType type, Tile tile, bool create = false)
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
        
        protected OsmGeo GetLocal(OsmGeoType type, long id)
        {
            var tile = new Tile(0, 0, 0);
            var index = LoadIndex(type, tile);

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
                index = LoadIndex(type, tile);
            }

            return null;
        }

        protected IEnumerable<OsmGeo> GetLocalTile(Tile tile)
        {
            throw new NotImplementedException();
        }
    }
}