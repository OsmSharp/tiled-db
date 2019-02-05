using System.Collections.Generic;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// A database diff.
    /// </summary>
    /// <remarks>
    /// A diff contains only diff data.
    /// </remarks>
    public class DatabaseDiff : DatabaseBase, IDatabaseView
    {
        private readonly IDatabaseView _baseView;

        /// <summary>
        /// Creates a new database diff view.
        /// </summary>
        /// <param name="baseView">The base view.</param>
        /// <param name="path">The path.</param>
        /// <param name="zoom">The zoom level.</param>
        /// <param name="compressed">The compressed flag.</param>
        /// <param name="mapped">The mapped flag.</param>
        public DatabaseDiff(IDatabaseView baseView, string path, uint zoom = 14, bool compressed = true, bool mapped = true)
            : base(path,mapped, zoom, compressed)
        {
            _baseView = baseView;
        }

        /// <inheritdoc/>
        public OsmGeo Get(OsmGeoType type, long id)
        {
            return this.GetLocal(type, id);
        }

        /// <inheritdoc/>
        public IEnumerable<OsmGeo> GetTile(Tile tile)
        {
            return this.GetLocalTile(tile);
        }
    }
}