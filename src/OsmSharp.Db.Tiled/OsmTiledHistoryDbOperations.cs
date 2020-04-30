using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Contains common IO operations for OSM dbs.
    /// </summary>
    internal static class OsmTiledHistoryDbOperations
    {

        /// <summary>
        /// Tries to parse the data from the given path, return true if successful.
        /// </summary>
        /// <param name="path">The path to parse the date from.</param>
        /// <param name="dateTime">The date if true.</param>
        /// <returns>True if successful.</returns>
        public static bool TryParseOsmTiledDbPath(string path, out DateTime dateTime)
        {
            var dateTimeString = FileSystemFacade.FileSystem.LeafDirectoryName(path);

            if (!long.TryParse(dateTimeString, NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture,
                out var millisecondEpochs))
            {
                dateTime = default;
                return false;
            }

            dateTime = millisecondEpochs.FromUnixTime();
            return true;
        }

        /// <summary>
        /// Gets all the osm tiled db paths.
        /// </summary>
        /// <returns>An enumeration of all the valid paths.</returns>
        public static IEnumerable<(string path, DateTime pathTime)> GetOsmTiledDbPaths(string path)
        {
            var directories = FileSystemFacade.FileSystem.EnumerateDirectories(path);
            foreach (var directory in directories)
            {
                if (!TryParseOsmTiledDbPath(directory, out var pathTime)) continue;

                yield return (directory, pathTime);
            }
        }
        
        /// <summary>
        /// Writes db meta to disk.
        /// </summary>
        /// <param name="path">The db path.</param>
        /// <param name="dbMeta">The meta-data to write.</param>
        public static void SaveDbMeta(string path, OsmTiledHistoryDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using var stream = FileSystemFacade.FileSystem.Open(dbMetaPath, FileMode.Create);
            using var streamWriter = new StreamWriter(stream);
            JsonSerializer.CreateDefault().Serialize(streamWriter, dbMeta);
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