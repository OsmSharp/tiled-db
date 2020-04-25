using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.Streams;

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
        /// <param name="settings">The settings.</param>
        /// <returns>A tiled history db.</returns>
        /// <exception cref="DirectoryNotFoundException"></exception>
        public static async Task<OsmTiledHistoryDb> Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14,
            OsmTiledHistoryDbSettings settings = null)
        {
            settings ??= new OsmTiledHistoryDbSettings();
            
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tiledOsmDbPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, DateTime.Now);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tiledOsmDbPath))
                FileSystemFacade.FileSystem.CreateDirectory(tiledOsmDbPath);

            // build the tiled db.
            await OsmTiled.Build.OsmTiledDbBuilder.Build(source, tiledOsmDbPath, zoom);

            // generate and write the path.
            var osmDbMeta = new OsmTiledHistoryDbMeta()
            {
                Latest = FileSystemFacade.FileSystem.RelativePath(path, tiledOsmDbPath)
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
        /// <param name="settings">The settings.</param>
        /// <returns>A new osm tiled db.</returns>
        /// <exception cref="DirectoryNotFoundException"></exception>
        public static async Task<OsmTiledDb> Update(this IEnumerable<OsmGeo> source, string path, uint zoom = 14,
            OsmTiledHistoryDbSettings settings = null)
        {
            settings ??= new OsmTiledHistoryDbSettings();
            
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
                throw new DirectoryNotFoundException(
                    $"Cannot create OSM db: {path} not found.");

            var tiledOsmDbPath = OsmTiledHistoryDbOperations.BuildOsmTiledDbPath(path, DateTime.Now);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tiledOsmDbPath))
                FileSystemFacade.FileSystem.CreateDirectory(tiledOsmDbPath);

            // build the tiled db.
            await OsmTiled.Build.OsmTiledDbBuilder.Build(source, tiledOsmDbPath, zoom);

            return new OsmTiledDb(tiledOsmDbPath, new OsmTiledDbSettings()
            {
                AsReader = settings.AsReader
            });
        }
    }
}