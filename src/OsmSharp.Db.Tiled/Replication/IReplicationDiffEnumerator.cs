//using System;
//using System.Threading.Tasks;
//using OsmSharp.Changesets;
//
//namespace OsmSharp.Db.Tiled.Replication
//{
//    /// <summary>
//    /// Abstract representation of a replication diff enumerator.
//    /// </summary>
//    public interface IReplicationDiffEnumerator
//    {
//        /// <summary>
//        /// Moves to the next diff, returns true when it's available.
//        /// </summary>
//        /// <returns></returns>
//        Task<bool> MoveNext();
//
//        /// <summary>
//        /// Gets the replication state.
//        /// </summary>
//        ReplicationState State { get; }
//
//        /// <summary>
//        /// Gets the replication config.
//        /// </summary>
//        ReplicationConfig Config { get; }
//    }
//    
//    /// <summary>
//    /// Contains extension methods for the replication diff enumerator.
//    /// </summary>
//    public static class IReplicationChangesetEnumeratorExtensions
//    {
//        /// <summary>
//        /// Downloads the diff based on the current state of the enumerator.
//        /// </summary>
//        /// <param name="enumerator">The enumerator.</param>
//        /// <returns>The diff.</returns>
//        public static async Task<OsmChange> Diff(this IReplicationDiffEnumerator enumerator)
//        {
//            return await enumerator.Config.DownloadDiff(enumerator.State.SequenceNumber);
//        }
//    }
//}