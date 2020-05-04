using System;
using System.Collections.Generic;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    internal static class OsmTiledDbDiffBuilder
    {
        /// <summary>
        /// Builds a diff db and writes the structure to the given path.
        /// </summary>
        /// <param name="osmTiledDb">The tiled base db.</param>
        /// <param name="changeset">The changeset stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        /// <param name="buffer">The buffer.</param>
        /// <param name="settings">The settings.</param>
        /// <returns>Meta data on the new tiled db.</returns>
        public static OsmTiledDbMeta BuildDiff(this OsmTiledDbBase osmTiledDb, OsmChange changeset, string path,
            OsmTiledDbBuildSettings? settings = null, DateTime? timeStamp = null, byte[]? buffer = null)
        {
            settings ??= new OsmTiledDbBuildSettings();
            buffer ??= new byte[1024];
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);

            var zoom = osmTiledDb.Zoom;

            // collect all affected tiles and tile mutations.
            // build a modifications stream, a sorted stream augmented with tile ids. 
            // build up the entire stream in memory, we need all the tiles have have been modified.
            var modifiedTimeStamp = DateTime.MinValue;
            var modifications = new List<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)>();
            foreach (var modification in changeset.BuildTiledDiffStream(zoom, osmTiledDb, buffer))
            {
                // update timestamp.
                if (modification.osmGeo.TimeStamp.HasValue &&
                    modification.osmGeo.TimeStamp > modifiedTimeStamp)
                {
                    modifiedTimeStamp = modification.osmGeo.TimeStamp.Value;
                }
                
                // apply settings.
                settings.Prepare(modification.osmGeo);
                
                // add the modification.
                modifications.Add(modification);
            }

            // write the data.
            Log.Default.Verbose($"Writing {modifications.Count} modifications...");
            modifications.Write(path, zoom, saveDeleted: true, buffer: buffer);

            // update timestamp properly.
            timeStamp ??= modifiedTimeStamp;

            var id = timeStamp.Value.ToUnixTime();
            if (id == osmTiledDb.Id) throw new Exception("Timestamp has not moved!");

            // save the meta-data.
            var meta = new OsmTiledDbMeta
            {
                Id = id,
                Base = osmTiledDb.Id,
                Type = OsmTiledDbType.Diff,
                Zoom = zoom,
            };
            OsmTiledDbOperations.SaveDbMeta(path, meta);
            return meta;
        }
    }
}