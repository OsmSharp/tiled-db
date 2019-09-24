using System;
using System.Threading;
using System.Threading.Tasks;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// A replication changeset enumerator.
    /// </summary>
    internal class ReplicationChangesetEnumerator : IReplicationChangesetEnumerator
    {
        internal ReplicationChangesetEnumerator(ReplicationConfig config, long sequenceNumber)
        {
            Config = config;
            _lastReturned = sequenceNumber;
        }
        
        private long _lastReturned;
        private long _highestLatest = -1;

        /// <summary>
        /// Moves to the next diff, returns true when it's available.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> MoveNext()
        {
            if (_highestLatest < 0)
            {
                var latest = await Config.LatestReplicationState();
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
                    await Task.Delay((Config.Period / 10) * 1000);
                    var latest = await Config.LatestReplicationState();
                    _highestLatest = latest.SequenceNumber;
                }

                _lastReturned = next;
            }
            
            // download all the things.
            Current = await Config.DownloadDiff(_lastReturned);
            State = await Config.GetReplicationState(_lastReturned);
            IsLatest = (_lastReturned == _highestLatest);
            return true;
        }

        /// <summary>
        /// Gets the current diff.
        /// </summary>
        public OsmChange Current { get; private set; }

        /// <summary>
        /// Gets the replication config.
        /// </summary>
        public ReplicationConfig Config { get; }

        /// <summary>
        /// Gets the replication state.
        /// </summary>
        public ReplicationState State { get; private set; }
        
        /// <summary>
        /// Returns true if the current state is the latest.
        /// </summary>
        public bool IsLatest { get; private set; }
    }
}