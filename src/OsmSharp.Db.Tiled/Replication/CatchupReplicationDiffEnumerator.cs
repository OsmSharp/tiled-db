//using System;
//using System.Threading.Tasks;
//
//namespace OsmSharp.Db.Tiled.Replication
//{
//    /// <summary>
//    /// A replication enumerator to enumerate the minimum number of changeset to get updates since the given date time.
//    /// </summary>
//    public class CatchupReplicationDiffEnumerator : IReplicationDiffEnumerator
//    {
//        private readonly bool _moveDown;
//        
//        /// <summary>
//        /// Creates a new catch up replication changeset enumerator.
//        /// </summary>
//        /// <param name="dateTime">The timestamp to start at.</param>
//        /// <param name="moveDown">Move down from daily to hourly to minutely if latest is reached.</param>
//        public CatchupReplicationDiffEnumerator(DateTime dateTime, bool moveDown = true)
//        {
//            _startDateTime = dateTime;
//            _moveDown = moveDown;
//        }
//
//        private DateTime _startDateTime;
//        private ReplicationDiffEnumerator? _enumerator = null;
//
//        /// <summary>
//        /// Move to the next changeset.
//        /// </summary>
//        /// <returns>Returns true if a next changeset is available.</returns>
//        public async Task<bool> MoveNext()
//        {
//            if (_startDateTime.Minute != 0)
//            {
//                if (_enumerator != null && _enumerator.Config.IsMinutely)
//                {
//                    // move to the next minute.
//                    if (_enumerator.CurrentIsLatest)
//                    {
//                        // if latest, don't move anymore.
//                        return false;
//                    }
//
//                    if (!await _enumerator.MoveNext())
//                    {
//                        // move failed
//                        return false;
//                    }
//                }
//                else
//                {
//                    // move to the first minute.
//                    _enumerator = await Replication.Minutely.GetDiffEnumerator(_startDateTime);
//                    if (_enumerator == null)
//                    {
//                        // if no more minutely, then no more change sets.
//                        return false;
//                    }
//                }
//
//                // the new start date time is the end of the current diff.
//                _startDateTime = _enumerator.State.Timestamp;
//
//                return true;
//            }
//
//            if (_startDateTime.Hour != 0)
//            {
//                if (_enumerator != null && _enumerator.Config.IsHourly)
//                {
//                    // move to the next hour.
//                    if (_enumerator.CurrentIsLatest)
//                    {
//                        // if latest, try minutes.
//                        _enumerator = await Replication.Minutely.GetDiffEnumerator(_startDateTime);
//                        if (_enumerator != null && !_enumerator.CurrentIsLatest)
//                        { // there is an minute, maybe the next minute is there.
//                            if (!await _enumerator.MoveNext())
//                            {
//                                return false;
//                            }
//                        }
//                    }
//                    else
//                    {
//                        if (!await _enumerator.MoveNext())
//                        {
//                            // move failed
//                            return false;
//                        }
//                    }
//                }
//                else
//                {
//                    // move to the first hour.
//                    // do hours until day is 0.
//                    _enumerator = await Replication.Hourly.GetDiffEnumerator(_startDateTime);
//                    if (_enumerator == null)
//                    {
//                        if (!_moveDown) return false;
//                        
//                        // no more hourly, try minutely.
//                        _enumerator = await Replication.Minutely.GetDiffEnumerator(_startDateTime);
//                        if (_enumerator == null)
//                        {
//                            // if no more minutely, then no more change sets.
//                            return false;
//                        }
//                    }
//                }
//
//                // the new start date time is the end of the current diff.
//                _startDateTime = _enumerator.State.Timestamp;
//
//                return true;
//            }
//
//            // do daily until no more available.
//            if (_enumerator != null && _enumerator.Config.IsDaily)
//            {
//                // move to the next minute.
//                if (_enumerator.CurrentIsLatest)
//                {
//                    if (!_moveDown) return false;
//                    
//                    // if latest, try hours.
//                    _enumerator = await Replication.Hourly.GetDiffEnumerator(_startDateTime);
//                    if (_enumerator != null && !_enumerator.CurrentIsLatest)
//                    { // there is an hour, maybe the next hour is there.
//                        if (!await _enumerator.MoveNext())
//                        {
//                            // if latest, try minutes.
//                            _enumerator = await Replication.Minutely.GetDiffEnumerator(_startDateTime);
//                            if (_enumerator != null && !_enumerator.CurrentIsLatest)
//                            { // there is an minute, maybe the next minute is there.
//                                if (!await _enumerator.MoveNext())
//                                {
//                                    return false;
//                                }
//                            }
//                        }
//                    }
//                }
//                else
//                {
//                    if (!await _enumerator.MoveNext())
//                    {
//                        // move failed
//                        return false;
//                    }
//                }
//            }
//            else
//            {
//                _enumerator = await Replication.Daily.GetDiffEnumerator(_startDateTime);
//                if (_enumerator == null)
//                {
//                    if (!_moveDown) return false;
//                    
//                    // no more daily, try hourly.
//                    _enumerator = await Replication.Hourly.GetDiffEnumerator(_startDateTime);
//                    if (_enumerator == null)
//                    {
//                        // no more hourly, try minutely.
//                        _enumerator = await Replication.Minutely.GetDiffEnumerator(_startDateTime);
//                        if (_enumerator == null)
//                        {
//                            // if no more minutely, then no more change sets.
//                            return false;
//                        }
//                    }
//                }
//            }
//
//            // the new start date time is the end of the current diff.
//            _startDateTime = _enumerator.State.Timestamp;
//
//            return true;
//        }
//
//        /// <summary>
//        /// Gets the start date time.
//        /// </summary>
//        public DateTime Start => _startDateTime;
//        
//        /// <summary>
//        /// Gets the replication state.
//        /// </summary>
//        public ReplicationState State => _enumerator.State;
//        
//        /// <summary>
//        /// Gets the replication config.
//        /// </summary>
//        public ReplicationConfig Config => _enumerator.Config;
//    }
//}