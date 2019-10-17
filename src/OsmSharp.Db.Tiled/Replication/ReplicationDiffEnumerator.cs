using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using OsmSharp.Changesets;

[assembly: InternalsVisibleTo("OsmSharp.Db.Tiled.Tests")]
[assembly: InternalsVisibleTo("OsmSharp.Db.Tiled.Tests.Functional")]
namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// A replication changeset enumerator.
    /// </summary>
    public class ReplicationDiffEnumerator : IReplicationDiffEnumerator
    {
        internal ReplicationDiffEnumerator(ReplicationConfig config)
        {
            Config = config;
            _lastReturned = -1;
        }
        
        private long _lastReturned;
        private long _highestLatest = -1;

        /// <summary>
        /// Moves this enumerator to the given sequence number.
        /// </summary>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <returns>True if the move was a success, false otherwise. Throw an exception on anything but a 404 from the server.</returns>
        internal async Task<bool> MoveTo(long sequenceNumber)
        {
            var state = await Config.GetReplicationState(sequenceNumber);
            if (state == null) return false;
            
            _lastReturned = sequenceNumber;
            State = state;

            return true;
        }

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
                    await Task.Delay(Math.Min(60 * 1000, Config.Period * 1000 / 10));
                    var latest = await Config.LatestReplicationState();
                    _highestLatest = latest.SequenceNumber;
                }

                _lastReturned = next;
            }
            
            // download all the things.
            State = await Config.GetReplicationState(_lastReturned);
            IsLatest = (_lastReturned == _highestLatest);
            return true;
        }

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