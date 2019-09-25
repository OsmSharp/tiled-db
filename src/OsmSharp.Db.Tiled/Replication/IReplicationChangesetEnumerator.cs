using System.Threading.Tasks;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// Abstract representation of a replication changeset enumerator.
    /// </summary>
    public interface IReplicationChangesetEnumerator
    {
        /// <summary>
        /// Moves to the next diff, returns true when it's available.
        /// </summary>
        /// <returns></returns>
        Task<bool> MoveNext();

        /// <summary>
        /// Gets the replication state.
        /// </summary>
        ReplicationState State { get; }

        /// <summary>
        /// Gets the replication config.
        /// </summary>
        ReplicationConfig Config { get; }
    }
    
    /// <summary>
    /// Contains extension methods for the replication changeset enumerator.
    /// </summary>
    public static class IReplicationChangesetEnumeratorExtensions
    {
        /// <summary>
        /// Downloads the diff based on the current state of the enumerator.
        /// </summary>
        /// <param name="enumerator">The enumerator.</param>
        /// <returns>The diff.</returns>
        public static async Task<OsmChange> Diff(this IReplicationChangesetEnumerator enumerator)
        {
            return await enumerator.Config.DownloadDiff(enumerator.State.SequenceNumber);
        }
    }
}