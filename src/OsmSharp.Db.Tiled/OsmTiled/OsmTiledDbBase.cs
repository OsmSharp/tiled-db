using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        protected OsmTiledDbBase(string path, OsmTiledDbMeta meta)
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
        /// Gets the object for the given type/id.
        /// </summary>
        /// <param name="osmGeoKey">The key.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object if present.</returns>
        public virtual OsmGeo? Get(OsmGeoKey osmGeoKey, byte[]? buffer = null)
        {
            return this.Get(new[] {osmGeoKey}, buffer).FirstOrDefault();
        }
        
        /// <summary>
        /// Gets the objects for the given keys.
        /// </summary>
        /// <param name="osmGeoKeys">The keys.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object(s) if present.</returns>
        public abstract IEnumerable<OsmGeo> Get(IReadOnlyCollection<OsmGeoKey> osmGeoKeys, byte[]? buffer = null);

        /// <summary>
        /// Gets all the tiles that have data, for a snapshot these are only the modified tiles.
        /// </summary>
        /// <returns>The tiles if any.</returns>
        public abstract IEnumerable<(uint x, uint y)> GetModifiedTiles();

        /// <summary>
        /// Get the tile the given object exists in, if any.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The tiles if any.</returns>
        public virtual IEnumerable<(uint x, uint y)> GetTiles(OsmGeoKey key)
        {
            foreach (var (x, y, _) in this.GetTiles(new[] {key}))
            {
                yield return (x, y);
            }
        }
        
        /// <summary>
        /// Gets the tiles for the objects with the given keys.
        /// </summary>
        /// <param name="osmGeoKeys">The keys.</param>
        /// <returns>All the tiles to objects are in.</returns>
        public abstract IEnumerable<(uint x, uint y, OsmGeoKey key)> GetTiles(IReadOnlyCollection<OsmGeoKey> osmGeoKeys);

        /// <summary>
        /// Gets all the data in the given tile(s).
        /// </summary>
        /// <param name="tiles">The tile(s).</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>All objects in the given tile(s).</returns>
        public abstract IEnumerable<(OsmGeo osmGeo, IReadOnlyCollection<(uint x, uint y)> tiles)> Get(IReadOnlyCollection<(uint x, uint y)> tiles, byte[]? buffer = null);
    }
}