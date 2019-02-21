using System;
using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// Keeps replication state.
    /// </summary>
    public class ReplicationState
    {
        internal static string SequenceNumberKey = "sequenceNumber";

        /// <summary>
        /// Creates a new replication state.
        /// </summary>
        /// <param name="sequenceNumber">The sequence number.</param>
        internal ReplicationState(long sequenceNumber)
        {
            this.SequenceNumber = sequenceNumber;
        }
        
        /// <summary>
        /// Gets the sequence number.
        /// </summary>
        public long SequenceNumber { get; }
    }
}