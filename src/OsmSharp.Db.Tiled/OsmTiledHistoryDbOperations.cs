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
        /// <param name="id">The id if true.</param>
        /// <returns>True if successful.</returns>
        public static bool TryParseOsmTiledDbPath(string path, out long id)
        {
            id = default;
            var dateTimeString = FileSystemFacade.FileSystem.LeafDirectoryName(path);
            if (!(dateTimeString.EndsWith(OsmTiledDbType.Full) || dateTimeString.EndsWith(OsmTiledDbType.Snapshot))) return false;
            if(dateTimeString == null) return false;
            var lastIndexOf= dateTimeString.LastIndexOf("_", StringComparison.Ordinal);
            if (lastIndexOf <= 0) return false;
            
            if (!long.TryParse(dateTimeString.Substring(0, lastIndexOf), NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture,
                out var millisecondEpochs))
            {
                return false;
            }

            id = millisecondEpochs;
            return true;
        }

        /// <summary>
        /// Gets all the osm tiled db paths.
        /// </summary>
        /// <returns>An enumeration of all the valid paths.</returns>
        public static IEnumerable<(long id, string path)> GetOsmTiledDbPaths(string path)
        {
            var directories = FileSystemFacade.FileSystem.EnumerateDirectories(path);
            foreach (var directory in directories)
            {
                if (!TryParseOsmTiledDbPath(directory, out var id)) continue;

                yield return (id, directory);
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