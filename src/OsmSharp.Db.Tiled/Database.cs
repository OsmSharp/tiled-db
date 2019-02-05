using OsmSharp.Db.Tiled.Ids;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using Serilog;

namespace OsmSharp.Db.Tiled
{
    public class Database
    {
        private readonly string _path;
        private readonly bool _compressed;
        private readonly uint _zoom;
        private const uint ZoomOffset = 2;
        private const bool _mapped = true;

        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _nodeIndexesCache;
        private readonly ConcurrentDictionary<uint, LRUCache<ulong, Index>> _wayIndexesCache;

        /// <summary>
        /// Creates a new data based on the given folder.
        /// </summary>
        public Database(string folder, uint zoom = 12, bool compressed = true)
        {
            // TODO: verify that zoom offset leads to zoom zero from the given zoom level here.
            // in other words, zoom level has to be exactly dividable by ZoomOffset.
            _path = folder;
            _compressed = compressed;
            _zoom = zoom;

            _nodeIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
            _wayIndexesCache = new ConcurrentDictionary<uint, LRUCache<ulong, Index>>();
        }

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        public uint Zoom => _zoom;
        
        /// <summary>
        /// Gets the node with given id.
        /// </summary>
        public Node GetNode(long id)
        {
            var tile = new Tile(0, 0, 0);
            var index = LoadIndex(OsmGeoType.Node, tile);

            while (index != null &&
                   index.TryGetMask(id, out var mask))
            {
                var subTiles = tile.SubTilesForMask2(mask);
                var subTile = subTiles.First();

                if (subTile.Zoom == _zoom)
                { // load data and find node.
                    var stream = DatabaseCommon.LoadTile(_path, OsmGeoType.Node, subTile, _compressed);
                    if (stream == null)
                    {
                        Log.Warning($"Could not find sub tile, it should be there: {subTile}");
                        return null;
                    }
                    using (stream)
                    {
                        var source = new OsmSharp.Streams.BinaryOsmStreamSource(stream);
                        while (source.MoveNext(false, true, true))
                        {
                            var current = source.Current();

                            if (current.Id == id)
                            {
                                return current as Node;
                            }
                        }
                    }
                }

                tile = subTile;
                index = LoadIndex(OsmGeoType.Node, tile);
            }

            return null;
        }

        /// <summary>
        /// Loads the index for the given type and tile.
        /// </summary>
        private Index LoadIndex(OsmGeoType type, Tile tile, bool create = false)
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
        
        /// <summary>
        /// Gets all the relevant tiles.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Tile> GetTiles()
        {
            var basePath = FileSystemFacade.FileSystem.Combine(_path, _zoom.ToInvariantString());
            if (!FileSystemFacade.FileSystem.DirectoryExists(basePath))
            {
                yield break;
            }
            var mask = "*.nodes.osm.bin";
            if (_compressed) mask = mask + ".zip";
            foreach(var xDir in FileSystemFacade.FileSystem.EnumerateDirectories(
                basePath))
            {
                var xDirName = FileSystemFacade.FileSystem.DirectoryName(xDir);
                if (!uint.TryParse(xDirName, out var x))
                {
                    continue;
                }

                foreach (var tile in FileSystemFacade.FileSystem.EnumerateFiles(xDir, mask))
                {
                    var tileName = FileSystemFacade.FileSystem.FileName(tile);

                    if (!uint.TryParse(tileName.Substring(0,
                        tileName.IndexOf('.')), out var y))
                    {
                        continue;
                    }

                    yield return new Tile(x, y, _zoom);
                }
            }
        }
    }
}