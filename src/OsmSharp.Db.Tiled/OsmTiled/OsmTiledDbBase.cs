using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary>
    /// Represents a snapshot of OSM data at a given point in time.
    /// </summary>
    public abstract class OsmTiledDbBase
    {
        private readonly OsmTiledDbMeta _meta;

        protected OsmTiledDbBase(string path)
            : this(path, OsmTiledDbOperations.LoadDbMeta(path))
        {

        }

        internal OsmTiledDbBase(string path, OsmTiledDbMeta meta)
        {
            Path = path;
            _meta = meta;
        }

        /// <summary>
        /// Gets the path.
        /// </summary>
        internal string Path { get; }

        /// <summary>
        /// Gets the zoom.
        /// </summary>
        public uint Zoom => _meta.Zoom;

        /// <summary>
        /// Gets the id.
        /// </summary>
        internal long Id => _meta.Id;

        /// <summary>
        /// Gets the timespan.
        /// </summary>
        internal long? Timespan => _meta.Timespan;

        /// <summary>
        /// Gets the meta data.
        /// </summary>
        public IEnumerable<(string key, string value)> Meta => _meta.GetMeta();

        /// <summary>
        /// Gets the start timestamp, if any.
        /// </summary>
        public DateTime? StartTimestamp
        {
            get
            {
                if (this.Timespan == null) return null;

                return this.EndTimestamp.Subtract(TimeSpan.FromMilliseconds(this.Timespan.Value));
            }
        }

        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public DateTime EndTimestamp => _meta.Timestamp;

        /// <summary>
        /// Gets the base.
        /// </summary>
        internal long? Base => _meta.Base;

        /// <summary>
        /// Gets the db containing the latest version of the data for the given tile.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <returns>The database containing the latest version of the given tile.</returns>
        public abstract OsmTiledDbBase? GetDbForTile((uint x, uint y) tile);

        /// <summary>
        /// Gets the data for the given keys.
        /// </summary>
        /// <param name="osmGeoKeys">The keys, gets all data when null.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object(s) if present.</returns>
        public virtual IEnumerable<OsmGeo> GetOsmGeo(IEnumerable<OsmGeoKey>? osmGeoKeys,
            byte[]? buffer = null)
        {
            foreach (var (osmGeo, _) in this.Get(osmGeoKeys))
            {
                yield return osmGeo;
            }
        }
        
        /// <summary>
        /// Gets the data for the given keys.
        /// </summary>
        /// <param name="osmGeoKeys">The keys, gets all data when null.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object(s) if present.</returns>
        public abstract IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<OsmGeoKey>? osmGeoKeys,
            byte[]? buffer = null);

        /// <summary>
        /// Gets all the data.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        /// <returns>All objects.</returns>
        public abstract IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(byte[]? buffer = null);

        /// <summary>
        /// Gets all the data in the given tile(s).
        /// </summary>
        /// <param name="tiles">The tile(s).</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>All objects in the given tile(s).</returns>
        public abstract IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> Get(IEnumerable<(uint x, uint y)> tiles, byte[]? buffer = null);

        /// <summary>
        /// Get the tiles that have data, if any.
        /// </summary>
        /// <returns>The tiles if any.</returns>
        public abstract IEnumerable<(uint x, uint y)> GetTiles(bool modifiedOnly = false);
        
        /// <summary>
        /// Gets the tiles for the objects with the given keys.
        /// </summary>
        /// <param name="osmGeoKeys">The keys.</param>
        /// <returns>All the tiles to objects are in.</returns>
        public abstract IEnumerable<(OsmGeoKey key, IEnumerable<(uint x, uint y)> tiles)> GetTilesFor(IEnumerable<OsmGeoKey> osmGeoKeys);
    }
}