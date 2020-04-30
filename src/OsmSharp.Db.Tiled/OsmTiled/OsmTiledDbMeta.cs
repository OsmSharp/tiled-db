using System;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary>
    /// Represents meta data about a snapshot db.
    /// </summary>
    public class OsmTiledDbMeta
    {
        /// <summary>
        /// Gets or sets the id of this db.
        /// </summary>
        public long Id { get; set; }
        
        /// <summary>
        /// Get or sets the zoom level.
        /// </summary>
        public uint Zoom { get; set; }
        
        /// <summary>
        /// The base db id if any.
        /// </summary>
        public long? Base { get; set; }

        /// <summary>
        /// The db type.
        /// </summary>
        public string Type { get; set; } = OsmTiledDbType.Full;

        /// <summary>
        /// Gets or sets the 
        /// </summary>
        public DateTime Timestamp => this.Id.FromUnixTime();
    }
}