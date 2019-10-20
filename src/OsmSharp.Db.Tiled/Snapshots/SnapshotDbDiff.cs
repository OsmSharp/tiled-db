using System.Collections.Generic;
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

        /// <inheritdoc/>
        public override OsmGeo Get(OsmGeoType type, long id)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type)
        {
            throw new System.NotImplementedException();
        }
    }
}