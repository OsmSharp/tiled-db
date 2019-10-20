using System;

namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary>
    /// Represents meta data about a snapshot db.
    /// </summary>
    public class SnapshotDbMeta
    {
        /// <summary>
        /// Get or sets the zoom level.
        /// </summary>
        public uint Zoom { get; set; }
        
        /// <summary>
        /// The base db if any.
        /// </summary>
        public string Base { get; set; }
        
        /// <summary>
        /// The db type.
        /// </summary>
        public string Type { get; set; }
        
        /// <summary>
        /// Gets or sets the 
        /// </summary>
        public DateTime Timestamp { get; set; }
    }
}