using System.Collections.Generic;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a diff relative to another snapshot.
    /// </summary>
    public class SnapshotDbDiff : SnapshotDb
    {
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        public SnapshotDbDiff(string path)
            : base(path)
        {
            
        }

        internal SnapshotDbDiff(string path, SnapshotDbMeta meta)
            : base(path, meta)
        {
            
        }

        /// <inheritdoc/>
        public override OsmGeo Get(OsmGeoType type, long id)
        {
            var local = this.GetLocal(type, id);
            if (local != null) return local;

            // move to base.
            var b = SnapshotDbOperations.LoadDb(this.Base);
            return b.Get(type, id);
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type)
        {
            var local = SnapshotDbOperations.GetLocalTile(this.Path, this.Zoom, tile, type);
            if (local != null) return local;

            // move to base.
            var b = SnapshotDbOperations.LoadDb(this.Base);
            return b.GetTile(tile, type);
        }
    }
}