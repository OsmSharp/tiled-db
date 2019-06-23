using System;
using System.Threading.Tasks;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// A replication enumerator to enumerate the minimum number of changeset to get updates since the given date time.
    /// </summary>
    public class CatchupReplicationChangesetEnumerator : IReplicationChangesetEnumerator
    {
        /// <summary>
        /// Creates a new catch up replication changeset enumerator.
        /// </summary>
        /// <param name="dateTime"></param>
        public CatchupReplicationChangesetEnumerator(DateTime dateTime)
        {
            _startDateTime = dateTime;
        }

        private DateTime _startDateTime;
        private IReplicationChangesetEnumerator _enumerator = null;

        /// <summary>
        /// Move to the next changeset.
        /// </summary>
        /// <returns>Returns true if a next changeset is available.</returns>
        public async Task<bool> MoveNext()
        {
            if (_startDateTime.Minute != 0)
            { // first do minutes until hour is 0.
                _enumerator = await Replication.Minutely.GetDiffEnumerator(
                    await Replication.Minutely.SequenceNumberAt(_startDateTime));
                if (_enumerator == null)
                { // if no more minutely, then no more change sets.
                    return false;
                }
            }
            else
            {
                if (_startDateTime.Hour != 0)
                { // do hours until day is 0.
                    _enumerator = await Replication.Hourly.GetDiffEnumerator(
                        await Replication.Hourly.SequenceNumberAt(_startDateTime));
                    if (_enumerator == null)
                    { // no more hourly, try minutely.
                        _enumerator = await Replication.Minutely.GetDiffEnumerator(
                            await Replication.Minutely.SequenceNumberAt(_startDateTime));
                        if (_enumerator == null)
                        { // if no more minutely, then no more change sets.
                            return false;
                        }
                    }
                }
                else
                { // do daily until no more available.
                    _enumerator = await Replication.Daily.GetDiffEnumerator(
                        await Replication.Daily.SequenceNumberAt(_startDateTime));
                    if (_enumerator == null)
                    { // no more daily, try hourly.
                        _enumerator = await Replication.Hourly.GetDiffEnumerator(
                            await Replication.Hourly.SequenceNumberAt(_startDateTime));
                        if (_enumerator == null)
                        { // no more hourly, try minutely.
                            _enumerator = await Replication.Minutely.GetDiffEnumerator(
                                await Replication.Minutely.SequenceNumberAt(_startDateTime));
                            if (_enumerator == null)
                            { // if no more minutely, then no more change sets.
                                return false;
                            }
                        }
                    }
                }
            }

            if (await _enumerator.MoveNext())
            {
                // the new start date time is the end of the current diff.
                _startDateTime = _enumerator.State.Timestamp
                    .AddSeconds(-_enumerator.State.Timestamp.Second);
                
                return true;
            }

            return false;
        }

        public DateTime Start => _startDateTime;
        
        /// <summary>
        /// Gets the current diff.
        /// </summary>
        public OsmChange Current => _enumerator.Current;
        
        /// <summary>
        /// Gets the replication state.
        /// </summary>
        public ReplicationState State => _enumerator.State;
        
        /// <summary>
        /// Gets the replication config.
        /// </summary>
        public ReplicationConfig Config => _enumerator.Config;
    }
}