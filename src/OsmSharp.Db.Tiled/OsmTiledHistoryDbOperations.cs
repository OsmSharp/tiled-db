using System;
using System.IO;
using Newtonsoft.Json;
using OsmSharp.Db.Tiled.IO;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Contains common IO operations for OSM dbs.
    /// </summary>
    internal static class OsmTiledHistoryDbOperations
    {
        /// <summary>
        /// Builds a path for a new tiled osm db for the give date time.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="dateTime">The date time.</param>
        /// <returns>A path.</returns>
        public static string BuildOsmTiledDbPath(string path, DateTime dateTime)
        {
            return FileSystemFacade.FileSystem.Combine(path,
                $"{dateTime.Year:0000}-{dateTime.Month:00}-{dateTime.Day:00}_{dateTime.Hour:00}{dateTime.Minute:00}{dateTime.Second:00}_{dateTime.Millisecond:0000}");
        }
        
        
        /// <summary>
        /// Writes db meta to disk.
        /// </summary>
        /// <param name="path">The db path.</param>
        /// <param name="dbMeta">The meta-data to write.</param>
        public static void SaveDbMeta(string path, OsmTiledHistoryDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using (var stream = FileSystemFacade.FileSystem.Open(dbMetaPath, FileMode.Create))
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
        public static OsmTiledHistoryDbMeta LoadDbMeta(string path)
        {
            var dbMetaPath = PathToMeta(path);
            using var stream = FileSystemFacade.FileSystem.OpenRead(dbMetaPath);
            using var streamReader = new StreamReader(stream);
            using var jsonReader = new JsonTextReader(streamReader);
            
            return JsonSerializer.CreateDefault().Deserialize<OsmTiledHistoryDbMeta>(jsonReader);
        }

        /// <summary>
        /// Gets the path to the meta-data for the db at the given path.
        /// </summary>
        public static string PathToMeta(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "meta.json");
        }
    }
}