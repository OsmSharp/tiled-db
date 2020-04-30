using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;

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
        /// <returns>A tiled history db.</returns>
        public static OsmTiledHistoryDb Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14)
        {
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tempPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, DateTime.Now, "temp");
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);

            // build the tiled db.
            var dbMeta = OsmTiled.Build.OsmTiledDbBuilder.Build(source, tempPath, zoom);

            // generate a proper path and move the data there.
            var dbPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, dbMeta.Timestamp, OsmTiledDbType.Full);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);

            // generate and write the path.
            var osmDbMeta = new OsmTiledHistoryDbMeta()
            {
                Latest = FileSystemFacade.FileSystem.RelativePath(path, dbPath)
            };
            OsmTiledHistoryDbOperations.SaveDbMeta(path, osmDbMeta);

            // return the osm db.
            return new OsmTiledHistoryDb(path);
        }
        
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source data.</param>
        /// <param name="path">The path.</param>
        /// <param name="zoom">The zoom.</param>
        /// <returns>A new osm tiled db.</returns>
        /// <exception cref="DirectoryNotFoundException"></exception>
        public static OsmTiledDb Update(this IEnumerable<OsmGeo> source, string path, uint zoom = 14)
        {
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tempPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, DateTime.Now, OsmTiledDbType.Full);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);

            // build the tiled db.
            var dbMeta = OsmTiled.Build.OsmTiledDbBuilder.Build(source, tempPath, zoom);

            // generate a proper path and move the data there.
            var dbPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, dbMeta.Timestamp, OsmTiledDbType.Full);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);

            // generate and write the path.
            var osmDbMeta = new OsmTiledHistoryDbMeta()
            {
                Latest = FileSystemFacade.FileSystem.RelativePath(path, dbPath)
            };
            OsmTiledHistoryDbOperations.SaveDbMeta(path, osmDbMeta);

            return new OsmTiledDb(dbPath);
        }
    }
}