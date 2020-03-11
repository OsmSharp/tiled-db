using System;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Contains extensions methods for the OsmDb.
    /// </summary>
    public static class OsmDbExtensions
    {
        /// <summary>
        /// Applies the current diff to the osmDb.
        /// </summary>
        /// <param name="osmDb">The osmDb.</param>
        /// <param name="enumerator">The enumerator.</param>
        public static async Task ApplyDiff(this OsmDb osmDb, CatchupReplicationDiffEnumerator enumerator)
        {
            osmDb.ApplyDiff(await enumerator.Diff(), enumerator.State.Timestamp);
        }

        /// <summary>
        /// Applies the current diff to the osmDb.
        /// </summary>
        /// <param name="osmDb">The osmDb.</param>
        /// <param name="enumerator">The enumerator.</param>
        public static async Task ApplyCurrent(this CatchupReplicationDiffEnumerator enumerator, OsmDb osmDb)
        {
            await osmDb.ApplyDiff(enumerator);
        }
            
        /// <summary>
        /// Applies the current diff to the osmDb.
        /// </summary>
        /// <param name="osmDb">The osmDb.</param>
        /// <param name="enumerator">The enumerator.</param>
        public static async Task ApplyDiff(this OsmDb osmDb, ReplicationDiffEnumerator enumerator)
        {
            osmDb.ApplyDiff(await enumerator.Diff(), enumerator.State.Timestamp);
        }

        /// <summary>
        /// Gets the snapshot containing the data for the given date time.
        /// </summary>
        /// <param name="osmDb">The osmDb.</param>
        /// <param name="dateTime">The date time.</param>
        /// <returns>The snapshot if any for the given data.</returns>
        public static OsmSharp.Db.Tiled.Snapshots.SnapshotDb GetAt(this OsmDb osmDb, DateTime dateTime)
        {
            var snapshotDb = osmDb.Latest;
            if (dateTime <= snapshotDb.Timestamp)
            {
                return snapshotDb;
            }

            while (snapshotDb != null &&
                   snapshotDb.Timestamp > dateTime)
            {
                snapshotDb = snapshotDb.GetBaseDb();
            }

            return snapshotDb;
        }
    }
}