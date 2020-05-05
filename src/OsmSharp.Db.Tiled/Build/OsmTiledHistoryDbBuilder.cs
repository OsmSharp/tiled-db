using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.Build
{
    /// <summary>
    /// Contains code to build an OSM db.
    /// </summary>
    internal static class OsmTiledHistoryDbBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source data.</param>
        /// <param name="path">The path.</param>
        /// <param name="zoom">The zoom.</param>
        /// <param name="timeStamp">The timestamp, overrides the timestamps in the data.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        /// <returns>A tiled history db.</returns>
        public static OsmTiledHistoryDb Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14, DateTime? timeStamp = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tempPath = OsmTiledDbOperations.BuildTempDbPath(path);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);

            // build the tiled db.
            var dbMeta = OsmTiled.Build.OsmTiledDbBuilder.Build(source, tempPath, zoom, timeStamp: timeStamp, meta: meta);

            // generate a proper path and move the data there.
            var dbPath = OsmTiledDbOperations.BuildDbPath(path, dbMeta.Id, null, OsmTiledDbType.Full);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);

            // return the osm db.
            return new OsmTiledHistoryDb(path);
        }
        
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source data.</param>
        /// <param name="path">The path.</param>
        /// <param name="zoom">The zoom.</param>
        /// <param name="timeStamp">The timestamp, overrides the timestamps in the data.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        /// <returns>A new osm tiled db.</returns>
        /// <exception cref="DirectoryNotFoundException"></exception>
        public static OsmTiledDb Add(this IEnumerable<OsmGeo> source, string path, uint zoom = 14, DateTime? timeStamp = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tempPath = FileSystemFacade.FileSystem.Combine(path, Guid.NewGuid().ToString());
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);

            // build the tiled db.
            var dbMeta = OsmTiled.Build.OsmTiledDbBuilder.Build(source, tempPath, zoom, timeStamp: timeStamp, meta: meta);

            // generate a proper path and move the data there.
            var dbPath = OsmTiledDbOperations.BuildDbPath(path, dbMeta.Id, null, OsmTiledDbType.Full);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);

            return new OsmTiledDb(dbPath);
        }
    }
}