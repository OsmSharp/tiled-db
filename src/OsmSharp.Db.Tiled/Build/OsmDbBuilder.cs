using System;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Build
{
    /// <summary>
    /// Contains code to build an OSM db.
    /// </summary>
    public static class OsmDbBuilder
    {
        private const string InitialSnapshotDbPath = "initial";

        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        public static async Task<OsmTiledHistoryDb> BuildDb(this OsmStreamSource source, string path, uint zoom = 14)
        {
            if (zoom % 2 != 0) throw new ArgumentException($"{nameof(zoom)} max zoom has to be a multiple of 2."); 
            
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) throw new DirectoryNotFoundException(
                $"Cannot create OSM db: {path} not found.");
            
            var snapshotDbPath = FileSystemFacade.FileSystem.Combine(path, InitialSnapshotDbPath);
            if (!FileSystemFacade.FileSystem.DirectoryExists(snapshotDbPath)) FileSystemFacade.FileSystem.CreateDirectory(snapshotDbPath);

            // build the snapshot db.
            await source.Build(snapshotDbPath, zoom);
            
            // generate and write the path.
            var osmDbMeta = new OsmTiledHistoryDbMeta()
            {
                Latest = snapshotDbPath
            };
            OsmDbOperations.SaveDbMeta(path, osmDbMeta);
            
            // return the osm db.
            return new OsmTiledHistoryDb(path);
        }
    }
}