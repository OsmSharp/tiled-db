using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db snapshot from an OSM changeset.
    /// </summary>
    internal static class OsmTiledDbSnapshotBuilder
    {
        /// <summary>
        /// Builds a snapshot db from a changeset on top of the given db.
        /// </summary>
        /// <param name="osmTiledDb">The tiled db.</param>
        /// <param name="changeset">The changeset stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        /// <param name="buffer">The buffer.</param>
        /// <param name="settings">The settings.</param>
        /// <returns>Meta data on the new tiled db.</returns>
        public static OsmTiledDbMeta BuildSnapshot(this OsmTiledDbBase osmTiledDb, OsmChange changeset, string path,
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
            var modifiedTiles = new HashSet<(uint x, uint y)>();
            var modifications = new List<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)>();
            foreach (var modification in changeset.BuildTiledDiffStream(zoom,
                (key) => osmTiledDb.GetTiles(key.Type, key.Id)))
            {
                // update timestamp.
                if (modification.osmGeo.TimeStamp.HasValue &&
                    modification.osmGeo.TimeStamp > modifiedTimeStamp)
                {
                    modifiedTimeStamp = modification.osmGeo.TimeStamp.Value;
                }
                
                // keep modified tiles.
                modifiedTiles.UnionWith(modification.tiles);
                
                // apply settings.
                settings.Prepare(modification.osmGeo);
                
                // add the modification.
                modifications.Add(modification);
            }
            
            // merge the modifications stream with the data that is already there.
            var merged = osmTiledDb.Get(modifiedTiles)
                .ApplyTiledDiffStream(modifications);

            // write the data.
            merged.Write(path, zoom, false, modifiedTiles, buffer);
            
            // update timestamp properly.
            timeStamp ??= modifiedTimeStamp;

            var id = timeStamp.Value.ToUnixTime();
            if (id == osmTiledDb.Id) throw new Exception("Timestamp has not moved!");

            // save the meta-data.
            var meta = new OsmTiledDbMeta
            {
                Id = id,
                Base = osmTiledDb.Id,
                Type = OsmTiledDbType.Snapshot,
                Zoom = zoom,
            };
            OsmTiledDbOperations.SaveDbMeta(path, meta);
            return meta;
        }

        /// <summary>
        /// Takes a snapshot from the given db.
        /// </summary>
        /// <param name="osmTiledDb">The db to snapshot.</param>
        /// <param name="tiles">The tiles to snapshot for.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="id">The id of the new database.</param>
        /// <param name="baseId">The id of the new base.</param>
        /// <param name="buffer">The buffer.</param>
        /// <param name="settings">The settings.</param>
        /// <returns>Meta data on the new tiled db.</returns>
        public static OsmTiledDbMeta BuildSnapshot(this OsmTiledDbBase osmTiledDb,
            IReadOnlyCollection<(uint x, uint y)> tiles, string path, long id, long baseId, 
            OsmTiledDbBuildSettings? settings = null, byte[]? buffer = null)
        {
            settings ??= new OsmTiledDbBuildSettings();
            buffer ??= new byte[1024];
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);

            var zoom = osmTiledDb.Zoom;
            
            // merge the modifications stream with the data that is already there.
            var merged = osmTiledDb.Get(tiles);
            
            // apply settings.
            merged = merged.Select(x =>
            {
                settings.Prepare(x.osmGeo);

                return (x.osmGeo, x.tiles);
            });

            // write the data.
            merged.Write(path, zoom, false, tiles, buffer);

            // save the meta-data.
            var meta = new OsmTiledDbMeta
            {
                Id = id,
                Base = baseId,
                Type = OsmTiledDbType.Snapshot,
                Zoom = zoom,
            };
            OsmTiledDbOperations.SaveDbMeta(path, meta);
            return meta;
        }
    }
}