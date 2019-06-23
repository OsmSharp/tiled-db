using System;
using System.Diagnostics.SymbolStore;
using System.Globalization;
using System.IO;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class ReplicationStateExtensions
    {
        /// <summary>
        /// Parses a replication state from a state file stream.
        /// </summary>
        /// <param name="streamReader">The stream reader.</param>
        /// <returns>The replication state.</returns>
        /// <exception cref="Exception"></exception>
        public static ReplicationState ParseReplicationState(this StreamReader streamReader)
        {
            var sequenceNumber = long.MaxValue;
            var timestamp = default(DateTime);
            while (!streamReader.EndOfStream)
            {
                var line = streamReader.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("#")) continue;
                if (line.StartsWith(ReplicationState.SequenceNumberKey))
                { // this line has the sequence number.
                    var keyValue = line.Split('=');
                    if (keyValue.Length != 2) throw new Exception($"Could not parse {ReplicationState.SequenceNumberKey}");
                    if (!long.TryParse(keyValue[1], out sequenceNumber)) throw new Exception($"Could not parse {ReplicationState.SequenceNumberKey}");
                }
                else if (line.StartsWith(ReplicationState.TimestampKey))
                {
                    var keyValue = line.Split('=');
                    if (keyValue.Length != 2) throw new Exception($"Could not parse {ReplicationState.TimestampKey}");
                    keyValue[1] = keyValue[1].Replace("\\", string.Empty);
                    if (!DateTime.TryParse(keyValue[1], out timestamp)) throw new Exception($"Could not parse {ReplicationState.TimestampKey}");
                }
            }

            return new ReplicationState(sequenceNumber, timestamp);
        }

        /// <summary>
        /// Returns true if the given replication state represents a diff overlapping the given date/time.
        /// </summary>
        /// <param name="state">The replication state.</param>
        /// <param name="config">The associated config.</param>
        /// <param name="dateTime">The date/time.</param>
        /// <returns>True if the given date/time is in the range ]state.timestamp - period, state.timestamp].</returns>
        public static bool Overlaps(this ReplicationState state, ReplicationConfig config, DateTime dateTime)
        {
            var start = state.Timestamp.AddSeconds(-config.Period);
            var end = state.Timestamp;

            if (start < dateTime &&
                end >= dateTime)
            {
                return true;
            }

            return false;
        }
    }
}