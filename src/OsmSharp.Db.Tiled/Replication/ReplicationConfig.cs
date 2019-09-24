using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO.Http;

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

        private ReplicationState _state = null;

        /// <summary>
        /// Gets the latest replication state.
        /// </summary>
        /// <returns>The latest replication state.</returns>
        public async Task<ReplicationState> LatestReplicationState()
        {
            if (_state != null &&
                _state.Timestamp > DateTime.Now.AddSeconds(this.Period))
            { // there cannot be a new latest.
                return _state;
            }
            
            using (var stream = await HttpHandler.Default.GetStreamAsync(new Uri(new Uri(this.Url), "state.txt").ToString()))
            using (var streamReader = new StreamReader(stream))
            {
                _state =  this.ParseReplicationState(streamReader);
            }

            return _state;
        }

        /// <summary>
        /// Returns true if this config is daily.
        /// </summary>
        public bool IsDaily => this.Period == Replication.Daily.Period;

        /// <summary>
        /// Returns true if this config is hourly.
        /// </summary>
        public bool IsHourly => this.Period == Replication.Hourly.Period;

        /// <summary>
        /// Returns true if this config is minutely.
        /// </summary>
        public bool IsMinutely => this.Period == Replication.Minutely.Period;

        public override string ToString()
        {
            return $"{this.Url} ({this.Period}s)";
        }
    }
}