using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="zoom">The zoom.</param>
        /// <param name="settings">The settings.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        public static OsmTiledDbMeta Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14,
            OsmTiledDbBuildSettings? settings = null, DateTime? timeStamp = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) throw new ArgumentException("Output path does not exist.");

            settings ??= new OsmTiledDbBuildSettings();

            // build the tiled stream, apply settings and determine timestamp.
            var dataLatestTimeStamp = DateTime.MinValue;
            var tiledStream = source.ToTiledStream(zoom).Select(x =>
            {
                // update timestamp.
                if (x.osmGeo.TimeStamp.HasValue &&
                    x.osmGeo.TimeStamp > dataLatestTimeStamp)
                {
                    dataLatestTimeStamp = x.osmGeo.TimeStamp.Value;
                }
                
                settings.Prepare(x.osmGeo);
                return x;
            });
            
            // write to disk.
            tiledStream.Write(path, zoom);

            // choose proper timestamp.
            timeStamp ??= dataLatestTimeStamp;

            // save the meta-data.
            var meta = new OsmTiledDbMeta
            {
                Id = timeStamp.Value.ToUnixTime(),
                Base = null, // this is a full db.
                Type = OsmTiledDbType.Full,
                Zoom = zoom
            };
            OsmTiledDbOperations.SaveDbMeta(path, meta);
            return meta;
        }

    }
}