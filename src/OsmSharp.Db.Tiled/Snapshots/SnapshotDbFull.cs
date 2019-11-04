using System.Collections.Generic;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class SnapshotDbFull : SnapshotDb
    {
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        public SnapshotDbFull(string path)
            : base(path)
        {
            
        }

        internal SnapshotDbFull(string path, SnapshotDbMeta meta)
            : base(path, meta)
        {
            
        }

        
        /// <inheritdoc/>
        public override OsmGeo Get(OsmGeoType type, long id)
        { // in a snapshot the local tiles contain all data.
            return this.GetLocal(type, id);
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type)
        { // in a snapshot the local tiles contain all data.
            return SnapshotDbOperations.GetLocalTile(this.Path, this.Zoom, tile, type);
        }
    }
}