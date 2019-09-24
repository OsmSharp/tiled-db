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
        internal static string TimestampKey = "timestamp";

        /// <summary>
        /// Creates a new replication state.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="timestamp">The timestamp.</param>
        internal ReplicationState(ReplicationConfig config, long sequenceNumber, DateTime timestamp)
        {
            this.Config = config;
            this.SequenceNumber = sequenceNumber;
            this.Timestamp = timestamp;
        }
        
        /// <summary>
        /// Gets the replication config.
        /// </summary>
        public ReplicationConfig Config { get; }
        
        /// <summary>
        /// Gets the sequence number.
        /// </summary>
        public long SequenceNumber { get; }
        
        /// <summary>
        /// Gets the timestamp.
        /// </summary>
        public DateTime Timestamp { get; }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{this.SequenceNumber} @ {this.Timestamp} for {this.Config}";
        }
    }
}