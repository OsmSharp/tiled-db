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
            while (!streamReader.EndOfStream)
            {
                var line = streamReader.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("#")) continue;
                if (line.StartsWith(ReplicationState.SequenceNumberKey))
                { // this line has the sequence number.
                    var keyValue = line.Split('=');
                    if (keyValue == null || keyValue.Length != 2) throw new Exception($"Could not parse {ReplicationState.SequenceNumberKey}");
                    if (!long.TryParse(keyValue[1], out sequenceNumber)) throw new Exception($"Could not parse {ReplicationState.SequenceNumberKey}");
                }
            }

            return new ReplicationState(sequenceNumber);
        }
    }
}