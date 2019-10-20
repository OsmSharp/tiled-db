using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using Newtonsoft.Json;
using Reminiscence.Arrays;

namespace OsmSharp.Db.Tiled.Snapshots.IO
{
    /// <summary>
    /// Contains common db operations.
    /// </summary>
    internal static class SnapshotDbOperations
    {
        /// <summary>
        /// Writes db meta to disk.
        /// </summary>
        /// <param name="path">The db path.</param>
        /// <param name="dbMeta">The meta-data to write.</param>
        public static void SaveDbMeta(string path, SnapshotDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using (var stream = File.Open(dbMetaPath, FileMode.Create))
            using (var streamWriter = new StreamWriter(stream))
            {
                JsonSerializer.CreateDefault().Serialize(streamWriter, dbMeta);
            }
        }

        /// <summary>
        /// Loads db meta from disk.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>The db meta.</returns>
        public static SnapshotDbMeta LoadDbMeta(string path)
        {
            var dbMetaPath = PathToMeta(path);
            using (var stream = File.OpenRead(dbMetaPath))
            using (var streamReader = new StreamReader(stream))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                return JsonSerializer.CreateDefault().Deserialize<SnapshotDbMeta>(jsonReader);
            }
        }
        
        /// <summary>
        /// Get a stream to a tile at the given path.
        /// </summary>
        public static Stream LoadTile(string path, OsmGeoType type, Tile tile)
        {
            var location = PathToTile(path, type, tile);

            if (!FileSystemFacade.FileSystem.Exists(location))
            {
                return null;
            }

            return FileSystemFacade.FileSystem.OpenRead(location);
        }
        
        /// <summary>
        /// Creates a new tile on disk at the given path and returns a stream.
        ///
        /// Overwrites any tile that happened to be already there.
        /// </summary>
        public static Stream CreateTile(string path, OsmGeoType type, Tile tile)
        {
            var location = PathToTile(path, type, tile);

            var fileDirectory = FileSystemFacade.FileSystem.DirectoryForFile(location);
            if (!FileSystemFacade.FileSystem.DirectoryExists(fileDirectory))
            {
                FileSystemFacade.FileSystem.CreateDirectory(fileDirectory);
            }

            return FileSystemFacade.FileSystem.Open(location, FileMode.Create);
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
        public static string PathToTile(string path, OsmGeoType type, Tile tile)
        {
            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString());
            if (type == OsmGeoType.Node)
            {
                location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".nodes.osm.bin");
            }
            else if (type == OsmGeoType.Way)
            {
                location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".ways.osm.bin");
            }
            else
            {
                location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".relations.osm.bin");
            }
            
            return location;
        }
        
        /// <summary>
        /// Creates a local object.
        /// </summary>
        public static Stream CreateLocalTileObject(string path, Tile tile, OsmGeo osmGeo)
        {
            var location = BuildPathToLocalTileObject(path, tile, osmGeo);

            var fileDirectory = FileSystemFacade.FileSystem.DirectoryForFile(location);
            if (!FileSystemFacade.FileSystem.DirectoryExists(fileDirectory))
            {
                FileSystemFacade.FileSystem.CreateDirectory(fileDirectory);
            }

            return FileSystemFacade.FileSystem.Open(location, FileMode.Create);
        }

        /// <summary>
        /// Builds a path to a local object in the given tile.
        /// </summary>
        public static string BuildPathToLocalTileObject(string path, Tile tile, OsmGeo osmGeo, bool compressed = false)
        {
            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString(), tile.Y.ToInvariantString());
            
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.node.osm.bin");
                    break;
                case OsmGeoType.Way:
                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.way.osm.bin");
                    break;
                default:
                    location = FileSystemFacade.FileSystem.Combine(location, $"{osmGeo.Id.Value}.relation.osm.bin");
                    break;
            }

            if (compressed)
            {
                return location + ".zip";
            }
            return location;
        }
        
        /// <summary>
        /// Creates a tile.
        /// </summary>
        public static string PathToIndex(string path, OsmGeoType type, Tile tile)
        {
            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString());
            switch (type)
            {
                case OsmGeoType.Node:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".nodes.idx");
                    break;
                case OsmGeoType.Way:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".ways.idx");
                    break;
                default:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".relations.idx");
                    break;
            }
            return location;
        }
        
        /// <summary>
        /// Loads an index for the given tile from disk (if any).
        /// </summary>
        public static Index LoadIndex(string path, Tile tile, OsmGeoType type)
        {
            var extension = ".nodes.idx";
            switch (type)
            {
                case OsmGeoType.Way:
                    extension = ".ways.idx";
                    break;
                case OsmGeoType.Relation:
                    extension = ".relations.idx";
                    break;
            }

            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString(), tile.Y.ToInvariantString() + extension);
            if (!FileSystemFacade.FileSystem.Exists(location))
            {
                return null;
            }
            
            using (var stream = FileSystemFacade.FileSystem.OpenRead(location))
            {
                return Index.Deserialize(stream);
            }
        }
        
        /// <summary>
        /// Saves an index for the given tile to disk.
        /// </summary>
        public static void SaveIndex(string path, Tile tile, OsmGeoType type, Index index)
        {
            var extension = ".nodes.idx";
            switch (type)
            {
                case OsmGeoType.Way:
                    extension = ".ways.idx";
                    break;
                case OsmGeoType.Relation:
                    extension = ".relations.idx";
                    break;
            }

            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString(), tile.Y.ToInvariantString() + extension);
            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(location);
            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
            {
                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
            }
            using (var stream = FileSystemFacade.FileSystem.Open(location, FileMode.Create))
            {
                index.Serialize(stream);
            }
        }
        
        internal static IEnumerable<OsmGeo> GetLocalTile(string path, uint maxZoom, Tile tile, OsmGeoType type)
        {
            // TODO: dispose the returned streams, implement this in OSM stream source.
            if (tile.Zoom != maxZoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");

            var dataTile = SnapshotDbOperations.LoadTile(path, type, tile);
            if (dataTile == null) yield break;

            using (dataTile)
            {
                foreach (var osmGeo in new Streams.BinaryOsmStreamSource(dataTile))
                {
                    yield return osmGeo;
                }
            }
        }
        
        /// <summary>
        /// Loads a deleted index for the given tile from disk (if any).
        /// </summary>
        internal static DeletedIndex LoadDeletedIndex(string path, Tile tile, OsmGeoType type, bool mapped = false)
        {
            var location = PathToDeletedIndex(path, tile, type);
            if (!FileSystemFacade.FileSystem.Exists(location))
            {
                return null;
            }

            if (mapped)
            {
                var stream = FileSystemFacade.FileSystem.OpenRead(location);
                return DeletedIndex.Deserialize(stream, ArrayProfile.NoCache);
            }
            using (var stream = FileSystemFacade.FileSystem.OpenRead(location))
            {
                return DeletedIndex.Deserialize(stream);
            }
        }

        /// <summary>
        /// Saves a deleted index for the given tile to disk.
        /// </summary>
        internal static void SaveDeletedIndex(string path, Tile tile, OsmGeoType type, DeletedIndex deletedIndex)
        {
            var location = PathToDeletedIndex(path, tile, type);
            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(location);
            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
            {
                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
            }

            using (var stream = FileSystemFacade.FileSystem.Open(location, FileMode.Create))
            {
                deletedIndex.Serialize(stream);
            }
        }

        /// <summary>
        /// Builds a path to a deleted index.
        /// </summary>
        public static string PathToDeletedIndex(string path, Tile tile, OsmGeoType type)
        {
            var location = FileSystemFacade.FileSystem.Combine(path, tile.Zoom.ToInvariantString(),
                tile.X.ToInvariantString());
            switch (type)
            {
                case OsmGeoType.Node:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".nodes.idx.deleted");
                    break;
                case OsmGeoType.Way:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".ways.idx.deleted");
                    break;
                default:
                    location = FileSystemFacade.FileSystem.Combine(location, tile.Y.ToInvariantString() + ".relations.idx.deleted");
                    break;
            }
            return location;
        }
        
//        /// <summary>
//        /// Opens a stream to append to a deleted index. Creates the index if it doesn't exist yet.
//        /// </summary>
//        /// <param name="path">The path.</param>
//        /// <param name="type">The object type.</param>
//        /// <param name="tile">The tile.</param>
//        /// <returns>The stream.</returns>
//        internal static Stream OpenAppendStreamDeletedIndex(string path, OsmGeoType type, Tile tile)
//        {
//            var indexPath = DatabaseCommon.PathToDeletedIndex(path, type, tile);
//
//            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(indexPath);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
//            }
//            
//            return FileSystemFacade.FileSystem.Open(indexPath, FileMode.Append);
//        }
        
//        /// <summary>
//        /// Opens a stream to append to an index. Creates the index if it doesn't exist yet.
//        /// </summary>
//        /// <param name="path">The path.</param>
//        /// <param name="type">The object type.</param>
//        /// <param name="tile">The tile.</param>
//        /// <returns>The stream.</returns>
//        internal static Stream OpenAppendStreamIndex(string path, OsmGeoType type, Tile tile)
//        {
//            var indexPath = DatabaseCommon.PathToIndex(path, type, tile);
//            if (!FileSystemFacade.FileSystem.Exists(indexPath))
//            {
//                return FileSystemFacade.FileSystem.Open(indexPath, FileMode.Create);
//            }
//
//            var stream = FileSystemFacade.FileSystem.OpenWrite(indexPath);
//            stream.Seek(stream.Length, SeekOrigin.End);
//            return stream;
//        }

        /// <summary>
        /// Opens a stream to append to a data tile. Creates the tile if it doesn't exist yet.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="type">The object type.</param>
        /// <param name="tile">The tile.</param>
        /// <returns>The stream.</returns>
        internal static Stream OpenAppendStreamTile(string path, OsmGeoType type, Tile tile)
        {
            var location = PathToTile(path, type, tile);
            var parentPath = FileSystemFacade.FileSystem.ParentDirectory(location);
            if (!FileSystemFacade.FileSystem.DirectoryExists(parentPath))
            {
                FileSystemFacade.FileSystem.CreateDirectory(parentPath);
            }
            return FileSystemFacade.FileSystem.Open(location, FileMode.Append);
        }
    }
}