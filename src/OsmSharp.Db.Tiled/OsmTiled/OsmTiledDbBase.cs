using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary>
    /// Represents a snapshot of OSM data at a given point in time.
    /// </summary>
    public abstract class OsmTiledDbBase
    {
        private readonly string _path;
        private readonly OsmTiledDbMeta _meta;

        protected OsmTiledDbBase(string path)
            : this(path, OsmTiledDbOperations.LoadDbMeta(path))
        {

        }

        protected OsmTiledDbBase(string path, OsmTiledDbMeta meta)
        {
            _path = path;
            _meta = meta;
        }

        /// <summary>
        /// Gets the path.
        /// </summary>
        internal string Path => _path;

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        public uint Zoom => _meta.Zoom;

        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public DateTime Timestamp => _meta.Timestamp;

        /// <summary>
        /// Gets the base.
        /// </summary>
        internal string? Base => _meta.Base;

        /// <summary>
        /// Gets the given object.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The object if present.</returns>
        public abstract Task<OsmGeo?> Get(OsmGeoType type, long id);

        /// <summary>
        /// Gets the tiles for the given object.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>All the tiles to object is in.</returns>
        public abstract Task<IEnumerable<(uint x, uint y)>> GetTiles(OsmGeoType type, long id);

        /// <summary>
        /// Gets all the data in the given tile.
        /// </summary>
        /// <param name="tile"></param>
        /// <returns></returns>
        public abstract Task<IEnumerable<OsmGeo>> Get((uint x, uint y) tile);
    }
}