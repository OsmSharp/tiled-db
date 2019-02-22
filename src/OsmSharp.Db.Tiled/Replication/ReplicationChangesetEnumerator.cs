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
        private long _highestLatest = -1;

        /// <summary>
        /// Moves to the next diff, returns true when it's available.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> MoveNext()
        {
            if (_highestLatest < 0)
            {
                var latest = await _config.GetLatestReplicationState();
                _highestLatest = latest.SequenceNumber;
            }

            Current = null;
            
            if (_lastReturned < 0)
            { // start from the latest.
                _lastReturned = _highestLatest;
            }
            else
            {
                // there is a sequence number, try to increase.
                var next = _lastReturned + 1;

                while (next > _highestLatest)
                { // keep waiting until next is latest.
                    await Task.Delay((_config.Period / 10) * 1000);
                    var latest = await _config.GetLatestReplicationState();
                    _highestLatest = latest.SequenceNumber;
                }

                _lastReturned = next;
            }
            
            // download all the things.
            Current = await _config.DownloadDiff(_lastReturned);
            State = await _config.GetReplicationState(_lastReturned);
            return true;
        }

        /// <summary>
        /// Gets the current diff.
        /// </summary>
        public OsmChange Current { get; private set; }

        /// <summary>
        /// Gets the replication state.
        /// </summary>
        public ReplicationState State { get; private set; }
    }
}