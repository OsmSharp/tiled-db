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
        
//        /// <summary>
//        /// Gets all the relevant tiles in sorted order to enable possible merging.
//        /// </summary>
//        internal static IEnumerable<Tile> GetIndexTiles(string path, uint zoom)
//        {
//            var basePath = FileSystemFacade.FileSystem.Combine(path, zoom.ToInvariantString());
//            if (!FileSystemFacade.FileSystem.DirectoryExists(basePath))
//            {
//                yield break;
//            }
//            
//            var mask = "*.nodes.idx";
//            var xDirs = FileSystemFacade.FileSystem.EnumerateDirectories(
//                basePath).ToList();
//            xDirs.Sort();
//            foreach(var xDir in xDirs)
//            {
//                var xDirName = FileSystemFacade.FileSystem.DirectoryName(xDir);
//                if (!uint.TryParse(xDirName, out var x))
//                {
//                    continue;
//                }
//
//                var yDirs = FileSystemFacade.FileSystem.EnumerateFiles(xDir, mask).ToList();
//                yDirs.Sort();
//                foreach (var tile in yDirs)
//                {
//                    var tileName = FileSystemFacade.FileSystem.FileName(tile);
//
//                    if (!uint.TryParse(tileName.Substring(0,
//                        tileName.IndexOf('.')), out var y))
//                    {
//                        continue;
//                    }
//
//                    yield return new Tile(x, y, zoom);
//                }
//            }
//        }
//        
//        /// <summary>
//        /// Get a stream to a tile at the given path.
//        /// </summary>
//        public static Stream LoadTile(string path, OsmGeoType type, Tile tile)
//        {
//            var location = PathToTile(path, type, tile);
//
//            if (!FileSystemFacade.FileSystem.Exists(location))
//            {
//                return null;
//            }
//
//            //return new GZipStream(FileSystemFacade.FileSystem.OpenRead(location), CompressionMode.Decompress);           
//            return FileSystemFacade.FileSystem.OpenRead(location);
//        }
        
//        /// <summary>
//        /// Creates a new tile on disk at the given path and returns a stream.
//        ///
//        /// Overwrites any tile that happened to be already there.
//        /// </summary>
//        public static Stream CreateTile(string path, OsmGeoType type, Tile tile)
//        {
//            var location = PathToTile(path, type, tile);
//
//            var fileDirectory = FileSystemFacade.FileSystem.DirectoryForFile(location);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(fileDirectory))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(fileDirectory);
//            }
//
//            //return new GZipStream(FileSystemFacade.FileSystem.Open(location, FileMode.Create), CompressionLevel.Fastest);
//            return FileSystemFacade.FileSystem.Open(location, FileMode.Create);
//        }

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
        
//        /// <summary>
//        /// Creates a local object.
//        /// </summary>
//        public static Stream CreateLocalTileObject(string path, Tile tile, OsmGeo osmGeo)
//        {
//            var location = BuildPathToLocalTileObject(path, tile, osmGeo);
//
//            var fileDirectory = FileSystemFacade.FileSystem.DirectoryForFile(location);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(fileDirectory))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(fileDirectory);
//            }
//
//            return FileSystemFacade.FileSystem.Open(location, FileMode.Create);
//        }

//        /// <summary>
//        /// Builds a path to a local object in the given tile.
//        /// </summary>
//        public static string BuildPathToLocalTileObject(string path, Tile tile, OsmGeo osmGeo)
//        {
//            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
//                tile.X.ToInvariantString(), tile.Y.ToInvariantString());
//            
//            switch (osmGeo.Type)
//            {
//                case OsmGeoType.Node:
//                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.node.osm.bin");
//                    break;
//                case OsmGeoType.Way:
//                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.way.osm.bin");
//                    break;
//                default:
//                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.relation.osm.bin");
//                    break;
//            }
//            
//            return location;
//        }
//        
//        /// <summary>
//        /// Creates a tile.
//        /// </summary>
//        public static string PathToIndex(string path, Tile tile)
//        {
//            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
//                tile.X.ToInvariantString());
//            
//            return FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".idx");
//        }
//        
//        /// <summary>
//        /// Loads an index for the given tile from disk (if any).
//        /// </summary>
//        public static OsmGeoKeyIndex LoadIndex(string path, Tile tile)
//        {
//            var extension = ".idx";
//
//            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
//                tile.X.ToInvariantString(), tile.Y.ToInvariantString() + extension);
//            if (!FileSystemFacade.FileSystem.Exists(location))
//            {
//                return null;
//            }
//
//            using var stream = FileSystemFacade.FileSystem.OpenRead(location);
//            return OsmGeoKeyIndex.Deserialize(stream);
//        }
//        
//        /// <summary>
//        /// Saves an index for the given tile to disk.
//        /// </summary>
//        public static void SaveIndex(string path, Tile tile, OsmGeoKeyIndex index)
//        {
//            var extension = ".idx";
//
//            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
//                tile.X.ToInvariantString(), tile.Y.ToInvariantString() + extension);
//            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(location);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
//            }
//
//            using var stream = FileSystemFacade.FileSystem.Open(location, FileMode.Create);
//            index.Serialize(stream);
//        }

//        internal static IEnumerable<OsmGeo> GetLocalTile(string path, uint maxZoom, Tile tile, OsmGeoType type)
//        {
//            // TODO: dispose the returned streams, implement this in OSM stream source.
//            if (tile.Zoom != maxZoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");
//
//            var dataTile = SnapshotDbOperations.LoadTile(path, type, tile);
//            if (dataTile == null) return null;
//            return GetTileData(dataTile);
//        }

//        private static IEnumerable<OsmGeo> GetTileData(Stream stream)
//        {
//            using (stream)
//            {
//                foreach (var osmGeo in new Streams.BinaryOsmStreamSource(stream))
//                {
//                    yield return osmGeo;
//                }
//            }
//        }

//        /// <summary>
//        /// Loads a deleted index for the given tile from disk (if any).
//        /// </summary>
//        internal static DeletedIndex LoadDeletedIndex(string path, Tile tile, OsmGeoType type, bool mapped = false)
//        {
//            var location = PathToDeletedIndex(path, tile, type);
//            if (!FileSystemFacade.FileSystem.Exists(location))
//            {
//                return null;
//            }
//
//            if (mapped)
//            {
//                var stream = FileSystemFacade.FileSystem.OpenRead(location);
//                return DeletedIndex.Deserialize(stream, ArrayProfile.NoCache);
//            }
//            using (var stream = FileSystemFacade.FileSystem.OpenRead(location))
//            {
//                return DeletedIndex.Deserialize(stream);
//            }
//        }
//
//        /// <summary>
//        /// Saves a deleted index for the given tile to disk.
//        /// </summary>
//        internal static void SaveDeletedIndex(string path, Tile tile, OsmGeoType type, DeletedIndex deletedIndex)
//        {
//            var location = PathToDeletedIndex(path, tile, type);
//            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(location);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
//            }
//
//            using (var stream = FileSystemFacade.FileSystem.Open(location, FileMode.Create))
//            {
//                deletedIndex.Serialize(stream);
//            }
//        }
//
//        /// <summary>
//        /// Builds a path to a deleted index.
//        /// </summary>
//        public static string PathToDeletedIndex(string path, Tile tile, OsmGeoType type)
//        {
//            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
//                tile.X.ToInvariantString());
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".nodes.idx.deleted");
//                    break;
//                case OsmGeoType.Way:
//                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".ways.idx.deleted");
//                    break;
//                default:
//                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".relations.idx.deleted");
//                    break;
//            }
//            return location;
//        }
    }
}