using System;
using System.Threading.Tasks;

namespace OsmSharp.Db.Tiled.Replication
{
    public static class ReplicationConfigExtensions
    {
        /// <summary>
        /// Returns the sequence number for the diff overlapping the given date time.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="dateTime">The date time.</param>
        /// <returns>The sequence number.</returns>
        public static async Task<long> SequenceNumberAt(this ReplicationConfig config, DateTime dateTime)
        {
            var latest = await config.LatestReplicationState();
            var start = latest.Timestamp.AddSeconds(-config.Period);
            var diff = (int)(start - dateTime).TotalSeconds;
            var leftOver = (diff % config.Period);
            var sequenceOffset = (diff - leftOver) / config.Period;

            return latest.SequenceNumber - sequenceOffset - 1;
        }
    }
}