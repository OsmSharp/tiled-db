using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    /// <summary>
    /// Contains common db operations.
    /// </summary>
    internal static class OsmTiledDbOperations
    {
        /// <summary>
        /// Writes db meta to disk.
        /// </summary>
        /// <param name="path">The db path.</param>
        /// <param name="dbMeta">The meta-data to write.</param>
        public static void SaveDbMeta(string path, OsmTiledDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using (var stream = FileSystemFacade.FileSystem.Open(dbMetaPath, FileMode.Create))
            using (var streamWriter = new StreamWriter(stream))
            {
                JsonSerializer.CreateDefault().Serialize(streamWriter, dbMeta);
            }
        }

        /// <summary>
        /// Loads db from meta.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>The snapshot db.</returns>
        public static OsmTiledDbBase LoadDb(string path)
        {
            var meta = OsmTiledDbOperations.LoadDbMeta(path);
            switch (meta.Type)
            {
//                case SnapshotDbType.Diff:
//                    return new SnapshotDbDiff(path, meta);
                case OsmTiledDbType.Full:
                    return new OsmTiledDb(path, meta);
            }
            
            throw new Exception("Could not determine db type from meta.");
        }

        /// <summary>
        /// Loads db meta from disk.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>The db meta.</returns>
        internal static OsmTiledDbMeta LoadDbMeta(string path)
        {
            var dbMetaPath = PathToMeta(path);
            using (var stream = FileSystemFacade.FileSystem.OpenRead(dbMetaPath))
            using (var streamReader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                return JsonSerializer.CreateDefault().Deserialize<OsmTiledDbMeta>(jsonReader);
            }
        }
        
        /// <summary>
        /// Gets all the relevant tiles.
        /// </summary>
        internal static IEnumerable<(uint x, uint y, uint zoom)> GetTiles(string path, uint zoom,
            string mask = "*.osm.tile")
        {
            var basePath = FileSystemFacade.FileSystem.Combine(path, zoom.ToInvariantString());
            if (!FileSystemFacade.FileSystem.DirectoryExists(basePath))
            {
                yield break;
            }
            
            foreach(var xDir in FileSystemFacade.FileSystem.EnumerateDirectories(
                basePath).ToList())
            {
                var xDirName = FileSystemFacade.FileSystem.LeafDirectoryName(xDir);
                if (!uint.TryParse(xDirName, out var x))
                {
                    continue;
                }

                foreach (var tile in FileSystemFacade.FileSystem.EnumerateFiles(xDir, mask).ToList())
                {
                    var tileName = FileSystemFacade.FileSystem.FileName(tile);

                    if (!uint.TryParse(tileName.Substring(0,
                        tileName.IndexOf('.')), out var y))
                    {
                        continue;
                    }

                    yield return (x, y, zoom);
                }
            }
        }
        
        /// <summary>
        /// Gets the path to the meta-data for the db at the given path.
        /// </summary>
        public static string PathToIndex(string path, OsmGeoType type)
        {
            return type switch
            {
                OsmGeoType.Node => FileSystemFacade.FileSystem.Combine(path, "nodes.idx"),
                OsmGeoType.Way => FileSystemFacade.FileSystem.Combine(path, "ways.idx"),
                OsmGeoType.Relation => FileSystemFacade.FileSystem.Combine(path, "relations.idx"),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
            };
        }

        /// <summary>
        /// Gets the path to the meta-data for the db at the given path.
        /// </summary>
        public static string PathToMeta(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "meta.json");
        }

        /// <summary>
        /// Gets the path to the given tile for the db at the given path.
        /// </summary>
        public static string PathToTile(string path, (uint x, uint y, uint zoom) tile, string extension = ".osm.tile")
        {
            var location = FileSystemFacade.FileSystem.Combine(path, tile.zoom.ToInvariantString(),
                tile.x.ToInvariantString());
            return FileSystemFacade.FileSystem.Combine(location, tile.y.ToInvariantString() + extension);
        }
    }
}