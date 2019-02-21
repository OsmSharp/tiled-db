using System;
using System.Threading;
using System.Threading.Tasks;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// A replication changeset enumerator.
    /// </summary>
    public class ReplicationChangesetEnumerator
    {
        private readonly ReplicationConfig _config;

        internal ReplicationChangesetEnumerator(ReplicationConfig config, long? sequenceNumber = null)
        {
            _config = config;
            if (sequenceNumber != null) _lastReturned = sequenceNumber.Value - 1;
        }
        
        private long _lastReturned = -1;
        private OsmChange _diff = null;

        /// <summary>
        /// Gets the current sequence number.
        /// </summary>
        public long SequenceNumber => _lastReturned;

        /// <summary>
        /// Moves to the next diff, returns true when it's available.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> MoveNext()
        {
            var latest = await _config.GetLatestReplicationState();
            _diff = null;
            
            if (_lastReturned < 0)
            { // start from the latest.
                _lastReturned = latest.SequenceNumber;
            }
            else
            {
                // there is a sequence number, try to increase.
                var next = _lastReturned + 1;

                while (next > latest.SequenceNumber)
                { // keep waiting until next is latest.
                    await Task.Delay((_config.Period / 10) * 1000);
                    latest = await _config.GetLatestReplicationState();
                }

                _lastReturned = next;
            }
            
            _diff = await _config.DownloadDiff(_lastReturned);
            return true;
        }

        /// <summary>
        /// Gets the current diff.
        /// </summary>
        public OsmChange Current => _diff;
    }
}