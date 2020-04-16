using System;
using System.Collections.Generic;
using System.Linq;
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
        internal override OsmGeo Get(OsmGeoType type, long id, Func<Tile, bool> isDeleted)
        { // in a snapshot the local tiles contain all data.
            return this.GetLocal(type, id, isDeleted).osmGeo;
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(uint x, uint y, OsmGeoType type)
        { // in a snapshot the local tiles contain all data.
            return SnapshotDbOperations.GetLocalTile(this.Path, this.Zoom, new Tile(x, y, this.Zoom), type);
        }

        /// <inheritdoc/>
        public override IEnumerable<Tile> GetChangedTiles()
        {
            return Enumerable.Empty<Tile>();
        }

        /// <inheritdoc/>
        internal override IEnumerable<Tile> GetChangedTilesSinceLatestDiff()
        {
            return Enumerable.Empty<Tile>();
        }

        /// <inheritdoc/>
        internal override IEnumerable<Tile> GetIndexesForZoom(uint zoom)
        {            
            return SnapshotDbOperations.GetIndexTiles(this.Path, zoom);
        }

        /// <inheritdoc/>
        internal override IEnumerable<(OsmGeoType type, long id, int mask)> GetSortedIndexData(Tile tile)
        {
            return this.LoadIndex(tile);
        }

        /// <inheritdoc/>
        internal override SnapshotDb GetBaseDb()
        {
            return null;
        }

        /// <inheritdoc/>
        internal override SnapshotDb GetLatestNonDiff()
        {
            return this;
        }
    }
}