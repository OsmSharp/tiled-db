using System;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// Replication configuration.
    /// </summary>
    public class ReplicationConfig
    {
        /// <summary>
        /// Creates a new replication config.
        /// </summary>
        /// <param name="url">The url.</param>
        /// <param name="period">The period.</param>
        public ReplicationConfig(string url, int period)
        {
            this.Url = url;
            this.Period = period;
        }
        
        /// <summary>
        /// Gets the url.
        /// </summary>
        public string Url { get; }
        
        /// <summary>
        /// Gets the replication period in seconds.
        /// </summary>
        public int Period { get; }

        /// <summary>
        /// Gets the default configuration for minutely updates.
        /// </summary>
        public static ReplicationConfig Minutely =>
            new ReplicationConfig("https://planet.openstreetmap.org/replication/minute/", 60);

        /// <summary>
        /// Gets the default configuration for hourly updates.
        /// </summary>
        public static ReplicationConfig Hourly => new ReplicationConfig("https://planet.openstreetmap.org/replication/hour/", 3600);
    }
}