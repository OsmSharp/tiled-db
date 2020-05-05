using System;
using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary>
    /// Represents meta data about a snapshot db.
    /// </summary>
    internal class OsmTiledDbMeta
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
        /// The db meta data.
        /// </summary>
        public string[]? Meta { get; set; }

        /// <summary>
        /// The db type.
        /// </summary>
        public string Type { get; set; } = OsmTiledDbType.Full;

        /// <summary>
        /// Gets or sets the 
        /// </summary>
        public DateTime Timestamp => this.Id.FromUnixTime();

        /// <summary>
        /// Gets or sets the timespan.
        /// </summary>
        public long? Timespan
        {
            get
            {
                if (this.Base == null) return null;

                return this.Id - this.Base.Value;
            }
        }

        internal IEnumerable<(string key, string value)> GetMeta()
        {
            if (this.Meta == null) yield break;
            
            for (var i = 0; i + 1 < this.Meta.Length; i += 2)
            {
                yield return (this.Meta[i], this.Meta[i + 1]);
            }
        }

        internal void SetMeta(IEnumerable<(string key, string value)>? meta)
        {
            if (meta == null)
            {
                this.Meta = null;
                return;
            }

            var metaList = new List<(string key, string value)>(meta);
            this.Meta = new string[metaList.Count * 2];
            for (var i = 0; i < metaList.Count; i++)
            {
                this.Meta[i * 2] = metaList[i].key;
                this.Meta[i * 2 + 1] = metaList[i].value;
            }
        }
    }
}