using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Replication;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.Functional
{
    public static class TestExtensions
    {
        public static void WriteToOsmXml(this IEnumerable<OsmGeo> osmGeos, string file)
        {
            using (var stream = File.Open(file, FileMode.Create))
            {
                var xmlTarget = new XmlOsmStreamTarget(stream);
                xmlTarget.Initialize();
                xmlTarget.RegisterSource(osmGeos);
                xmlTarget.Pull();
                xmlTarget.Close();
            }
        }
        
        /// <summary>
        /// Gets the diff enumerator moved to the first next diff using the info in the given db.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="db">The db.</param>
        public static async Task<ReplicationDiffEnumerator?> GetDiffEnumerator(this ReplicationConfig config, OsmTiledDbBase db)
        {
            var meta = db.Meta;
            var period = int.MinValue;
            var sequenceNumber = long.MinValue;
            foreach (var (k, v) in meta)
            {
                if (k == "period" &&
                    int.TryParse(v, out var vInt))
                {
                    period = vInt;
                }

                if (k == "sequence_number" &&
                    long.TryParse(v, out var vLong))
                {
                    sequenceNumber = vLong;
                }
            }

            if (period == config.Period &&
                sequenceNumber != long.MinValue)
            {
                // the sequence number should be there.
                return await config.GetDiffEnumerator(sequenceNumber);
            }

            return await config.GetDiffEnumerator(db.EndTimestamp);
        }
    }
}